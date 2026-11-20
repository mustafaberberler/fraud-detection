"""
Neo4j Graph Client — Fraud Ring Detection
==========================================
Writes the transactions coming from the Kafka consumer into a graph structure.
Detects fraud ring clusters by running real PageRank and Louvain
community detection algorithms with the Neo4j GDS (Graph Data Science) library.

Graph Schema:
  (Customer)-[:MADE]->(Transaction)-[:AT]->(Merchant)
  (Transaction {is_fraud, fraud_prob, amount, timestamp, ...})

  The Sparkov dataset contains Customer and Merchant entities.
  Sparkov has no Device/IP information — so the schema is Customer-Merchant focused.

GDS Integration:
  - Graph projection: Customer and Merchant nodes are projected in memory
    (in-memory) via the MADE/AT relationships.
  - PageRank: Runs weighted by the fraud_weight property written on the
    Transaction edges (relationshipWeightProperty), so the score measures
    "fraud-aware centrality" rather than "raw centrality".
  - Louvain: Fraud ring candidate clusters are found with community detection;
    for each community, the fraud rate is compared with the overall average
    (lift score) to assess whether the cluster is meaningful.
  - If GDS is not installed/reachable, the class automatically falls back to a
    simple Cypher aggregation (heuristic) and logs it.
"""

import time
import logging
from datetime import datetime

from config.settings import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

logger = logging.getLogger(__name__)

GDS_GRAPH_NAME = "fraudGraph"


class Neo4jClient:
    """
    Client that writes transactions to the Neo4j graph database and runs
    GDS-based graph analytics (PageRank, Louvain).
    """

    def __init__(self):
        self.driver = None
        self._connected = False
        self._gds_available = False

    def connect(self):
        """Connect to Neo4j, create indexes/constraints and check whether GDS is present."""
        try:
            from neo4j import GraphDatabase
            self.driver = GraphDatabase.driver(
                NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)
            )
            # Connection test
            with self.driver.session() as session:
                session.run("RETURN 1")

            self._create_indexes()
            self._connected = True
            logger.info(f"  ✓ Neo4j bağlantısı kuruldu: {NEO4J_URI}")

            self._check_gds()

        except Exception as e:
            logger.warning(f"  ⚠ Neo4j bağlantısı kurulamadı: {e}")
            logger.warning("    Graph analytics devre dışı, pipeline devam edecek.")
            self._connected = False

    def _check_gds(self):
        """Check whether the GDS library is installed."""
        try:
            with self.driver.session() as session:
                result = session.run("CALL gds.version() YIELD gdsVersion RETURN gdsVersion")
                version = result.single()["gdsVersion"]
                self._gds_available = True
                logger.info(f"  ✓ Neo4j GDS bulundu (v{version}) — PageRank/Louvain aktif")
        except Exception as e:
            self._gds_available = False
            logger.warning(
                f"  ⚠ Neo4j GDS bulunamadı ({e}). "
                "PageRank/Louvain heuristic fallback ile çalışacak."
            )

    def _create_indexes(self):
        """Create indexes and uniqueness constraints for performance."""
        constraints = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Customer) REQUIRE c.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (m:Merchant) REQUIRE m.id IS UNIQUE",
            "CREATE INDEX IF NOT EXISTS FOR (t:Transaction) ON (t.id)",
            "CREATE INDEX IF NOT EXISTS FOR (t:Transaction) ON (t.is_fraud)",
        ]
        with self.driver.session() as session:
            for q in constraints:
                try:
                    session.run(q)
                except Exception:
                    pass  # Don't raise an error if the constraint already exists
        logger.info("  ✓ Neo4j index/constraint'ler oluşturuldu")

    def write_transaction(self, transaction: dict, prediction: dict) -> float:
        """
        Write the transaction to the graph. Customer → Transaction → Merchant relationship.

        The `fraud_weight` property is written on the MADE and AT relationships
        (based on fraud_prob); this property is used so that PageRank
        runs in a weighted manner.

        Args:
            transaction: Raw transaction data (from the Sparkov CSV)
            prediction:  Model prediction result (output of inference.py)

        Returns:
            float: Neo4j write duration (seconds)
        """
        if not self._connected:
            return 0.0

        t_start = time.perf_counter()

        try:
            with self.driver.session() as session:
                session.execute_write(
                    self._create_transaction_subgraph,
                    transaction,
                    prediction,
                )
        except Exception as e:
            logger.error(f"Neo4j yazma hatası: {e}")
            return 0.0

        return time.perf_counter() - t_start

    @staticmethod
    def _create_transaction_subgraph(tx, transaction, prediction):
        """
        Create the Customer → Transaction → Merchant subgraph with a Cypher query.
        fraud_weight is written on the MADE/AT relationships (min 0.01, based on fraud_prob)
        — for PageRank weighting.
        """
        query = """
        MERGE (c:Customer {id: $customer_id})
        ON CREATE SET c.created_at = datetime()

        MERGE (m:Merchant {id: $merchant_id})
        ON CREATE SET
            m.category = $category,
            m.created_at = datetime()

        MERGE (t:Transaction {id: $tx_id})
        ON CREATE SET
            t.amount     = $amount,
            t.timestamp  = $timestamp,
            t.hour       = $hour,
            t.day_of_week = $day_of_week,
            t.is_fraud   = $is_fraud,
            t.fraud_prob = $fraud_prob,
            t.risk_level = $risk_level,
            t.ae_anomaly = $ae_anomaly,
            t.created_at = datetime()

        MERGE (c)-[r1:MADE]->(t)
        ON CREATE SET r1.fraud_weight = $fraud_weight
        MERGE (t)-[r2:AT]->(m)
        ON CREATE SET r2.fraud_weight = $fraud_weight

        WITH c, t
        WHERE t.is_fraud = true
        SET c.has_fraud = true,
            c.last_fraud_at = datetime()
        """

        # tx_id: use trans_id if present, otherwise the timestamp+cc_num+amt combination
        tx_id = str(
            transaction.get("trans_id") or
            transaction.get("trans_num") or
            f"{transaction.get('trans_date_trans_time', '')}_{transaction.get('cc_num', '')}_{transaction.get('amt', '')}"
        )

        fraud_prob = float(prediction.get("fraud_probability", 0))
        # If the weight gets close to 0, GDS can run into problems in some algorithms,
        # so a small floor value (0.01) is added.
        fraud_weight = max(fraud_prob ** 2, 0.0001)

        tx.run(query, {
            "customer_id": str(transaction.get("cc_num", transaction.get("customer_id", "unknown"))),
            "merchant_id": str(transaction.get("merchant", transaction.get("merchant_id", "unknown"))),
            "category":    str(transaction.get("category", "")),
            "tx_id":       tx_id,
            "amount":      float(transaction.get("amt", 0)),
            "timestamp":   transaction.get("trans_date_trans_time", datetime.utcnow().isoformat()),
            "hour":        int(transaction.get("hour", 0)),
            "day_of_week": int(transaction.get("day_of_week", 0)),
            "is_fraud":    bool(prediction.get("is_fraud", False)),
            "fraud_prob":  fraud_prob,
            "risk_level":  prediction.get("risk_level", "LOW"),
            "ae_anomaly":  float(prediction.get("ae_anomaly_score", 0)),
            "fraud_weight": fraud_weight,
        })

    # =========================================================================
    # Graph Analytics — GDS PageRank + Louvain Community Detection
    # =========================================================================

    def _project_graph(self, session):
        """
        Create the GDS in-memory graph projection (drop the old one first if it exists).

        IMPORTANT: Since the graph schema is tripartite, i.e.
        (Customer)-[:MADE]->(Transaction)-[:AT]->(Merchant), the `Transaction`
        node must also be included in the projection. If only ['Customer', 'Merchant']
        are projected, one end of the MADE/AT relationships (Transaction) stays
        outside the graph and these relationships never enter the projection — as a
        result, the Customer/Merchant nodes stay completely isolated from each other
        (PageRank giving every node the default (1 - damping) score,
        i.e. a constant 0.15, is a symptom of this isolation — this bug
        was found in a previous version and fixed here).
        """
        session.run(f"""
            CALL gds.graph.exists('{GDS_GRAPH_NAME}') YIELD exists
            WHERE exists
            CALL gds.graph.drop('{GDS_GRAPH_NAME}') YIELD graphName
            RETURN graphName
        """)
        session.run(f"""
            CALL gds.graph.project(
                '{GDS_GRAPH_NAME}',
                ['Customer', 'Merchant', 'Transaction'],
                {{
                    MADE: {{orientation: 'UNDIRECTED', properties: 'fraud_weight'}},
                    AT:   {{orientation: 'UNDIRECTED', properties: 'fraud_weight'}}
                }}
            )
        """)

    def run_pagerank(self) -> list:
        """
        Find the riskiest customers with fraud-weighted PageRank.

        If GDS is installed: `gds.pageRank.stream` is used with `fraud_weight`
        as the relationshipWeightProperty — i.e. the score is not pure connection
        centrality but centrality weighted towards fraud transactions.

        If GDS is not installed: falls back to a simple Cypher aggregation
        (fraud_count × avg_prob) and logs it (it is not scientifically
        equivalent to PageRank).
        """
        if not self._connected:
            return []

        if not self._gds_available:
            return self._run_pagerank_heuristic_fallback()

        try:
            with self.driver.session() as session:
                self._project_graph(session)

                result = session.run(f"""
                    CALL gds.pageRank.stream(
                        '{GDS_GRAPH_NAME}',
                        {{relationshipWeightProperty: 'fraud_weight'}}
                    )
                    YIELD nodeId, score
                    WITH gds.util.asNode(nodeId) AS node, score
                    WHERE node:Customer
                    RETURN node.id AS customer_id,
                           round(score, 6) AS pagerank_score
                    ORDER BY pagerank_score DESC
                    LIMIT 20
                """)
                return [dict(record) for record in result]

        except Exception as e:
            logger.error(f"GDS PageRank hatası: {e}, heuristic fallback'e geçiliyor")
            return self._run_pagerank_heuristic_fallback()

    def _run_pagerank_heuristic_fallback(self) -> list:
        """Simple Cypher-based risk score (heuristic) used when GDS is unavailable."""
        try:
            with self.driver.session() as session:
                result = session.run("""
                    MATCH (c:Customer)-[:MADE]->(t:Transaction)
                    WHERE t.is_fraud = true
                    WITH c, COUNT(t) AS fraud_count,
                         SUM(t.amount) AS fraud_total,
                         AVG(t.fraud_prob) AS avg_prob
                    RETURN c.id AS customer_id,
                           fraud_count,
                           round(fraud_total, 2) AS fraud_total,
                           round(avg_prob, 4) AS avg_fraud_prob,
                           fraud_count * avg_prob AS risk_score
                    ORDER BY risk_score DESC
                    LIMIT 20
                """)
                return [dict(record) for record in result]
        except Exception as e:
            logger.error(f"Heuristic PageRank hatası: {e}")
            return []

    def detect_fraud_rings(self, min_community_size: int = 3) -> list:
        """
        Find fraud ring candidates with Louvain community detection.

        If GDS is installed: communities are found with `gds.louvain.stream`, then
        the fraud rate is computed for each community and a "lift" (significance)
        score is derived relative to the overall average fraud rate. Only
        communities with lift > 1.0 (i.e. riskier than average) are returned
        as fraud ring candidates.

        If GDS is not installed: falls back to a simple approximation (heuristic)
        based on a "number of shared merchants" threshold.
        """
        if not self._connected:
            return []

        if not self._gds_available:
            return self._detect_fraud_rings_heuristic_fallback()

        try:
            with self.driver.session() as session:
                self._project_graph(session)

                # Overall (baseline) fraud rate — reference for computing lift
                baseline = session.run("""
                    MATCH (t:Transaction)
                    RETURN avg(CASE WHEN t.is_fraud THEN 1.0 ELSE 0.0 END) AS baseline_rate,
                           count(t) AS total_tx
                """).single()
                baseline_rate = baseline["baseline_rate"] or 0.0001

                # Assign communities with Louvain (written as a property)
                session.run(f"""
                    CALL gds.louvain.write(
                        '{GDS_GRAPH_NAME}',
                        {{
                            relationshipWeightProperty: 'fraud_weight',
                            writeProperty: 'community_id'
                        }}
                    )
                """)

                # For each community: customer count, total tx, fraud tx, fraud rate, lift
                result = session.run("""
                    MATCH (c:Customer)
                    WHERE c.community_id IS NOT NULL
                    MATCH (c)-[:MADE]->(t:Transaction)
                    WITH c.community_id AS community_id,
                         COUNT(DISTINCT c) AS n_customers,
                         COUNT(t) AS n_transactions,
                         SUM(CASE WHEN t.is_fraud THEN 1 ELSE 0 END) AS n_fraud
                    WHERE n_customers >= $min_size
                    WITH community_id, n_customers, n_transactions, n_fraud,
                         toFloat(n_fraud) / n_transactions AS fraud_rate
                    RETURN community_id,
                           n_customers,
                           n_transactions,
                           n_fraud,
                           round(fraud_rate, 4) AS fraud_rate
                    ORDER BY fraud_rate DESC
                    LIMIT 50
                """, min_size=min_community_size)

                rings = []
                for record in result:
                    d = dict(record)
                    d["baseline_fraud_rate"] = round(baseline_rate, 4)
                    d["lift"] = (
                        round(d["fraud_rate"] / baseline_rate, 2)
                        if baseline_rate > 0 else None
                    )
                    rings.append(d)

                # Count only communities riskier than the average as fraud ring candidates
                significant_rings = [r for r in rings if (r["lift"] or 0) > 1.0]

                if significant_rings:
                    logger.info(
                        f"  🔍 Louvain: {len(significant_rings)} anlamlı (lift>1.0) "
                        f"fraud ring community'si bulundu (toplam community: {len(rings)})"
                    )
                return significant_rings

        except Exception as e:
            logger.error(f"GDS Louvain hatası: {e}, heuristic fallback'e geçiliyor")
            return self._detect_fraud_rings_heuristic_fallback()

    def _detect_fraud_rings_heuristic_fallback(self, min_shared_merchants: int = 3) -> list:
        """
        Simple approximation used when GDS is unavailable: finds pairs of customers
        who committed fraud at the same merchants, based on a shared-merchant-count
        threshold. It does not have the mathematical foundation of real Louvain
        community detection; it is only a pairwise matching.
        """
        try:
            with self.driver.session() as session:
                result = session.run("""
                    MATCH (c1:Customer)-[:MADE]->(t1:Transaction)-[:AT]->(m:Merchant)
                          <-[:AT]-(t2:Transaction)<-[:MADE]-(c2:Customer)
                    WHERE t1.is_fraud = true
                      AND t2.is_fraud = true
                      AND c1.id < c2.id
                    WITH c1, c2, COLLECT(DISTINCT m.id) AS shared_merchants,
                         COUNT(DISTINCT m) AS n_shared
                    WHERE n_shared >= $min_shared
                    RETURN c1.id AS customer_1,
                           c2.id AS customer_2,
                           n_shared AS shared_merchant_count,
                           shared_merchants[..5] AS sample_merchants
                    ORDER BY n_shared DESC
                    LIMIT 50
                """, min_shared=min_shared_merchants)
                rings = [dict(record) for record in result]
                if rings:
                    logger.info(f"  🔍 (heuristic) {len(rings)} potansiyel fraud ring bağlantısı bulundu")
                return rings
        except Exception as e:
            logger.error(f"Heuristic fraud ring detection hatası: {e}")
            return []

    def detect_fraud_ring_pairs(self, min_shared_merchants: int = 3) -> list:
        """
        Public pairwise variant used by the dashboard API: pairs of customers who
        committed fraud at the same merchants (keys: customer_1, customer_2,
        shared_merchant_count, sample_merchants). Independent of GDS.
        """
        if not self._connected:
            return []
        return self._detect_fraud_rings_heuristic_fallback(min_shared_merchants)

    def get_customer_graph_features(self, customer_id: str) -> dict:
        """
        Extract graph-based risk features for a single customer.
        These features can be fed to the ML model as an additional signal.
        """
        if not self._connected:
            return {}

        try:
            with self.driver.session() as session:
                result = session.run("""
                    MATCH (c:Customer {id: $cid})-[:MADE]->(t:Transaction)
                    OPTIONAL MATCH (t)-[:AT]->(m:Merchant)
                    WITH c, t, m
                    RETURN
                        COUNT(t) AS total_tx_count,
                        SUM(CASE WHEN t.is_fraud THEN 1 ELSE 0 END) AS fraud_tx_count,
                        COUNT(DISTINCT m.id) AS unique_merchants,
                        ROUND(AVG(t.amount), 2) AS avg_amount,
                        ROUND(MAX(t.fraud_prob), 4) AS max_fraud_prob,
                        c.has_fraud AS has_prior_fraud,
                        c.community_id AS community_id
                """, cid=customer_id)

                record = result.single()
                if record:
                    return dict(record)
                return {}

        except Exception as e:
            logger.error(f"Graph feature çıkarma hatası: {e}")
            return {}

    def get_graph_stats(self) -> dict:
        """Overall graph statistics."""
        if not self._connected:
            return {"connected": False}

        try:
            with self.driver.session() as session:
                result = session.run("""
                    MATCH (c:Customer) WITH COUNT(c) AS customers
                    MATCH (m:Merchant) WITH customers, COUNT(m) AS merchants
                    MATCH (t:Transaction) WITH customers, merchants, COUNT(t) AS transactions
                    MATCH (t2:Transaction) WHERE t2.is_fraud = true
                    WITH customers, merchants, transactions, COUNT(t2) AS fraud_transactions
                    RETURN customers, merchants, transactions, fraud_transactions
                """)
                record = result.single()
                if record:
                    return {
                        "connected": True,
                        "gds_available": self._gds_available,
                        "customers": record["customers"],
                        "merchants": record["merchants"],
                        "transactions": record["transactions"],
                        "fraud_transactions": record["fraud_transactions"],
                    }
        except Exception as e:
            logger.error(f"Graph stats hatası: {e}")
        return {"connected": True, "gds_available": self._gds_available, "error": "stats query failed"}

    def close(self):
        """Close the connection (also drop the GDS in-memory graph, if any)."""
        if self.driver and self._gds_available:
            try:
                with self.driver.session() as session:
                    session.run(f"""
                        CALL gds.graph.exists('{GDS_GRAPH_NAME}') YIELD exists
                        WHERE exists
                        CALL gds.graph.drop('{GDS_GRAPH_NAME}') YIELD graphName
                        RETURN graphName
                    """)
            except Exception:
                pass
        if self.driver:
            self.driver.close()
            logger.info("Neo4j bağlantısı kapatıldı.")
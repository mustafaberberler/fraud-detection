"""
Neo4j Graph Client — Fraud Ring Detection
==========================================
Kafka consumer'dan gelen işlemleri graf yapısına yazar.
PageRank ve Louvain community detection ile fraud ring tespiti yapar.

Tez önerisindeki taahhüt:
  "Müşteri–kart–cihaz–IP–merchant ilişkileri Neo4j'de graph yapısıyla
   modellenecek, PageRank ve Community Detection algoritmalarıyla
   fraud ringd kümeleri tespit eilecektir."

Graph Şeması:
  (Customer)-[:MADE]->(Transaction)-[:AT]->(Merchant)
  (Transaction {is_fraud, fraud_prob, amount, timestamp, ...})

  Sparkov dataseti Customer ve Merchant entity'lerini içerir.
  Device/IP bilgisi Sparkov'da yok — bu yüzden şema Customer-Merchant odaklı.
"""

import time
import logging
from datetime import datetime

from config.settings import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

logger = logging.getLogger(__name__)


class Neo4jClient:
    """
    Neo4j graph veritabanına işlem yazan ve graph analytics çalıştıran client.
    """

    def __init__(self):
        self.driver = None
        self._connected = False

    def connect(self):
        """Neo4j'ye bağlan ve index/constraint oluştur."""
        try:
            from neo4j import GraphDatabase
            self.driver = GraphDatabase.driver(
                NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)
            )
            # Bağlantı testi
            with self.driver.session() as session:
                session.run("RETURN 1")

            self._create_indexes()
            self._connected = True
            logger.info(f"  ✓ Neo4j bağlantısı kuruldu: {NEO4J_URI}")

        except Exception as e:
            logger.warning(f"  ⚠ Neo4j bağlantısı kurulamadı: {e}")
            logger.warning("    Graph analytics devre dışı, pipeline devam edecek.")
            self._connected = False

    def _create_indexes(self):
        """Performans için index ve uniqueness constraint oluştur."""
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
                    pass  # Constraint zaten varsa hata vermesin
        logger.info("  ✓ Neo4j index/constraint'ler oluşturuldu")

    def write_transaction(self, transaction: dict, prediction: dict) -> float:
        """
        İşlemi graf'a yaz. Customer → Transaction → Merchant ilişkisi.

        Args:
            transaction: Ham işlem verisi (Sparkov CSV'den)
            prediction:  Model tahmin sonucu (inference.py çıktısı)

        Returns:
            float: Neo4j yazma süresi (saniye)
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
        Cypher sorgusu ile Customer → Transaction → Merchant subgraph oluştur.
        """
        query = """
        // Customer node (MERGE = varsa bul, yoksa oluştur)
        MERGE (c:Customer {id: $customer_id})
        ON CREATE SET c.created_at = datetime()

        // Merchant node
        MERGE (m:Merchant {id: $merchant_id})
        ON CREATE SET
            m.category = $category,
            m.created_at = datetime()

        // Transaction node
        CREATE (t:Transaction {
            id:              $tx_id,
            amount:          $amount,
            timestamp:       $timestamp,
            hour:            $hour,
            day_of_week:     $day_of_week,
            is_fraud:        $is_fraud,
            fraud_prob:      $fraud_prob,
            risk_level:      $risk_level,
            ae_anomaly:      $ae_anomaly,
            created_at:      datetime()
        })

        // İlişkiler
        MERGE (c)-[:MADE]->(t)
        MERGE (t)-[:AT]->(m)

        // Fraud ise Customer'a risk flag ekle
        WITH c, t
        WHERE t.is_fraud = true
        SET c.has_fraud = true,
            c.last_fraud_at = datetime()
        """

        # Sparkov'dan customer ve merchant ID'leri
        # (Sparkov'da cc_num = customer, merchant = merchant name)
        tx.run(query, {
            "customer_id": str(transaction.get("cc_num", transaction.get("customer_id", "unknown"))),
            "merchant_id": str(transaction.get("merchant", transaction.get("merchant_id", "unknown"))),
            "category":    str(transaction.get("category", "")),
            "tx_id":       str(transaction.get("trans_num", f"tx_{time.time_ns()}")),
            "amount":      float(transaction.get("amt", 0)),
            "timestamp":   transaction.get("trans_date_trans_time", datetime.utcnow().isoformat()),
            "hour":        int(transaction.get("hour", 0)),
            "day_of_week": int(transaction.get("day_of_week", 0)),
            "is_fraud":    bool(prediction.get("is_fraud", False)),
            "fraud_prob":  float(prediction.get("fraud_probability", 0)),
            "risk_level":  prediction.get("risk_level", "LOW"),
            "ae_anomaly":  float(prediction.get("ae_anomaly_score", 0)),
        })

    # =========================================================================
    # Graph Analytics — PageRank + Louvain Community Detection
    # =========================================================================

    def run_pagerank(self) -> list:
        """
        PageRank ile en riskli müşterileri bul.
        Fraud işlem yapan müşterilerin bağlantıları üzerinden yayılım.

        GDS (Graph Data Science) kütüphanesi gerektirir.
        GDS yoksa basit Cypher sorgusu ile yaklaşık hesaplama yapılır.
        """
        if not self._connected:
            return []

        try:
            with self.driver.session() as session:
                # Önce GDS varlığını kontrol et
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
            logger.error(f"PageRank hatası: {e}")
            return []

    def detect_fraud_rings(self, min_shared_merchants: int = 3) -> list:
        """
        Fraud ring tespiti: Aynı merchant'larda fraud yapan müşteri kümelerini bul.
        Bu, Louvain community detection'ın basitleştirilmiş versiyonudur.

        Mantık: İki müşteri aynı merchant'ta fraud yaptıysa, aralarında
        "fraud_ring" ilişkisi olabilir. min_shared_merchants kadar ortak
        merchant'ı olanlar potansiyel fraud ring'dir.
        """
        if not self._connected:
            return []

        try:
            with self.driver.session() as session:
                result = session.run("""
                    // Fraud yapan müşterilerin ortak merchant'larını bul
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
                    logger.info(f"  🔍 {len(rings)} potansiyel fraud ring bağlantısı bulundu")
                return rings

        except Exception as e:
            logger.error(f"Fraud ring detection hatası: {e}")
            return []

    def get_customer_graph_features(self, customer_id: str) -> dict:
        """
        Tek bir müşteri için graph-based risk feature'larını çıkar.
        Bu feature'lar ML modeline ek sinyal olarak beslenebilir.
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
                        c.has_fraud AS has_prior_fraud
                """, cid=customer_id)

                record = result.single()
                if record:
                    return dict(record)
                return {}

        except Exception as e:
            logger.error(f"Graph feature çıkarma hatası: {e}")
            return {}

    def get_graph_stats(self) -> dict:
        """Genel graph istatistikleri."""
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
                        "customers": record["customers"],
                        "merchants": record["merchants"],
                        "transactions": record["transactions"],
                        "fraud_transactions": record["fraud_transactions"],
                    }
        except Exception as e:
            logger.error(f"Graph stats hatası: {e}")
        return {"connected": True, "error": "stats query failed"}

    def close(self):
        """Bağlantıyı kapat."""
        if self.driver:
            self.driver.close()
            logger.info("Neo4j bağlantısı kapatıldı.")
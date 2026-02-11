import json
from kafka import KafkaConsumer
from neo4j import GraphDatabase
from src.graph.bootstrap_m2 import bootstrap_m2
from prometheus_client import start_http_server, Counter
import time
from prometheus_client import Histogram

bootstrap_m2()


transactions_total = Counter(
    "fraud_transactions_total",
    "Total number of processed fraud transactions"
)

neo4j_write_latency = Histogram(
    "neo4j_write_latency_seconds",
    "Time spent writing a transaction to Neo4j",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2)
)

# Kafka consumer
consumer = KafkaConsumer(
    "fraud-transactions",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

# Neo4j connection
driver = GraphDatabase.driver(
    "bolt://localhost:7687",
    auth=("neo4j", "password")
)

def write_transaction(tx, data):
    tx.run(
        """
        // 1️⃣ M0 Transaction Instance
        CREATE (ti:TransactionInstance {
            id: $transaction_id,
            amount: $amount,
            timestamp: $timestamp
        })

        // CREATE -> MATCH geçişi için WITH şart
        WITH ti

        // 2️⃣ M1 FinancialTransaction Event
        MATCH (evt:Event {name:'FinancialTransaction'})

        // 3️⃣ INSTANCE_OF ilişkisi
        MERGE (ti)-[:INSTANCE_OF]->(evt)

        // 4️⃣ Customer & Merchant
        MERGE (c:Customer {id: $customer_id})
        MERGE (m:Merchant {id: $merchant_id})

        MERGE (c)-[:INITIATES]->(ti)
        MERGE (ti)-[:TARGETS]->(m)
        """,
        **data
    )


start_http_server(8000)

print("Consumer started. Writing to Neo4j...")

with driver.session() as session:
    for message in consumer:
        transaction = message.value
        print("Received:", transaction)

        start_time = time.time()

        session.execute_write(write_transaction, transaction)

        elapsed = time.time() - start_time
        neo4j_write_latency.observe(elapsed)

        transactions_total.inc()
        
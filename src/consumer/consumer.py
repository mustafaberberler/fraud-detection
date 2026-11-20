"""
Kafka Consumer — Real-Time Fraud Detection Pipeline
========================================================
Reads from the raw-transactions topic and runs the full pipeline:

  Transaction → [AE anomaly] → [LightGBM prediction] → [SHAP explanation]
       → [Neo4j graph write] → [Drift accumulation]
       → writes to the fraud-results topic

Prometheus metrics are served from the /metrics endpoint.

Usage:
    python -m consumer.consumer

Targets in the thesis proposal:
  - p95 latency ≤ 200ms
  - throughput ≥ 1000 TPS
  - Real-time SHAP explanation
  - Neo4j graph update
  - PSI/KL drift monitoring
  - Analyst feedback loop
"""

import json
import time
import logging
import signal
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from prometheus_client import (
    start_http_server,
    Counter,
    Histogram,
    Gauge,
    Summary,
)

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPICS,
    KAFKA_CONSUMER_GROUP,
    PROMETHEUS_PORT,
)
from ml.inference import FraudInferenceEngine
from ml.drift_monitor import DriftMonitor
from graph.neo4j_client import Neo4jClient

# =============================================================================
# Logging
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# =============================================================================
# Prometheus Metrics
# =============================================================================
TRANSACTIONS_TOTAL = Counter(
    "fraud_transactions_total",
    "Total number of processed transactions",
    ["risk_level"],
)

FRAUD_DETECTED = Counter(
    "fraud_detected_total",
    "Number of detected frauds",
)

PIPELINE_LATENCY = Histogram(
    "pipeline_latency_seconds",
    "Full pipeline latency (AE→LightGBM→SHAP→Neo4j)",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0),
)

INFERENCE_LATENCY = Histogram(
    "inference_latency_seconds",
    "ML inference latency (AE→LightGBM→SHAP)",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.2),
)

NEO4J_LATENCY = Histogram(
    "neo4j_write_latency_seconds",
    "Neo4j write duration",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5),
)

DRIFT_ALERTS = Counter(
    "drift_alerts_total",
    "Total number of drift alerts",
    ["severity"],
)

THROUGHPUT_GAUGE = Gauge(
    "pipeline_throughput_tps",
    "Current throughput (TPS)",
)

# =============================================================================
# Graceful Shutdown
# =============================================================================
_running = True


def signal_handler(sig, frame):
    global _running
    logger.info("Shutdown sinyali alındı...")
    _running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


# =============================================================================
# Consumer Pipeline
# =============================================================================
class FraudDetectionConsumer:
    """
    Main Kafka consumer. Orchestrates all the components:
      - ML Inference (AE + LightGBM + SHAP)
      - Neo4j graph write
      - Drift monitoring
      - Prometheus metrics
      - Result publishing
    """

    def __init__(self):
        self.engine = FraudInferenceEngine()
        self.drift_monitor = DriftMonitor()
        self.neo4j = Neo4jClient()

        self.consumer = None
        self.producer = None

        self._processed = 0
        self._start_time = None

        self._dice_executor = ThreadPoolExecutor(max_workers=2)

    def initialize(self):
        """Initialize all the components."""
        logger.info("=" * 60)
        logger.info("  Fraud Detection Pipeline başlatılıyor...")
        logger.info("=" * 60)

        # 1. Load the ML models
        logger.info("\n[1/5] ML modelleri yükleniyor...")
        self.engine.load_models()

        # 2. Load the drift reference distributions
        logger.info("\n[2/5] Drift referans dağılımları yükleniyor...")
        self.drift_monitor.load_reference()

        # 3. Neo4j connection
        logger.info("\n[3/5] Neo4j bağlantısı kuruluyor...")
        self.neo4j.connect()

        # 4. Kafka consumer
        logger.info("\n[4/5] Kafka consumer oluşturuluyor...")
        self.consumer = KafkaConsumer(
            KAFKA_TOPICS["raw_transactions"],
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_CONSUMER_GROUP,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            max_poll_records=100,
            session_timeout_ms=30000,
        )
        logger.info(f"  ✓ Subscribed: {KAFKA_TOPICS['raw_transactions']}")

        # 5. Kafka producer (for publishing results)
        logger.info("\n[5/5] Kafka result producer oluşturuluyor...")
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            acks=1,
        )
        logger.info(f"  ✓ Result topics: {KAFKA_TOPICS['fraud_results']}, {KAFKA_TOPICS['drift_alerts']}")

        # Prometheus metrics server
        start_http_server(PROMETHEUS_PORT)
        logger.info(f"\n  ✓ Prometheus metrics: http://localhost:{PROMETHEUS_PORT}/metrics")

        logger.info("\n" + "=" * 60)
        logger.info("  Pipeline hazır. İşlem bekleniyor...")
        logger.info("=" * 60 + "\n")

    def run(self):
        """Ana consumer loop."""
        global _running
        self._start_time = time.time()

        try:
            while _running:
                # Poll with timeout (for graceful shutdown)
                records = self.consumer.poll(timeout_ms=1000)

                for topic_partition, messages in records.items():
                    for message in messages:
                        if not _running:
                            break
                        self._process_transaction(message.value)

        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt alındı.")
        finally:
            self._shutdown()

    def _process_transaction(self, transaction: dict):
        """
        Pass a single transaction through the full pipeline.

        Order:
          1. ML inference (AE → LightGBM → SHAP)
          2. Neo4j graph write
          3. Drift accumulation
          4. Write the result to the fraud-results topic
          5. Update the Prometheus metrics
        """

        # Generate a consistent unique ID (same formula as server.py normalize_transaction)
        ts_raw = str(transaction.get("trans_date_trans_time", ""))
        cust   = str(transaction.get("cc_num", ""))
        amt    = str(transaction.get("amt", ""))
        txn_id = f"txn_{abs(hash(ts_raw + cust + amt)) % 10**9:09d}"

        t_pipeline_start = time.perf_counter()

        # === 1. ML Inference ===
        t_inference_start = time.perf_counter()
        try:
            prediction = self.engine.predict(transaction)
        except Exception as e:
            logger.error(f"Inference hatası: {e}")
            return
        inference_time = time.perf_counter() - t_inference_start
        INFERENCE_LATENCY.observe(inference_time)

        # === 2. Neo4j Graph Write ===
        neo4j_time = self.neo4j.write_transaction(transaction, prediction)
        NEO4J_LATENCY.observe(neo4j_time)

        # === 3. Drift Accumulation ===
        self.drift_monitor.accumulate(transaction)

        # If it is time for a drift check
        if self.drift_monitor.should_check():
            drift_alert = self.drift_monitor.check_drift()
            if drift_alert:
                self._publish_drift_alert(drift_alert)

        # === 4. Publish the result ===
        result = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "transaction_id": txn_id,  # instead of trans_num
            "trans_id": txn_id,  # ← for Neo4j
            "customer_id": str(transaction.get("cc_num", "")),
            "merchant": transaction.get("merchant", ""),
            "amount": transaction.get("amt", 0),
            "prediction": {
                "fraud_probability": prediction["fraud_probability"],
                "is_fraud": prediction["is_fraud"],
                "risk_level": prediction["risk_level"],
                "threshold": prediction["threshold"],
            },
            "explanation": {
                "shap_top_features": prediction["shap_explanation"],
            },
            "ae_anomaly_score": prediction["ae_anomaly_score"],
            "latency": {
                "inference_ms": round(inference_time * 1000, 2),
                "neo4j_ms": round(neo4j_time * 1000, 2),
                "total_ms": round(
                    (time.perf_counter() - t_pipeline_start) * 1000, 2
                ),
            },
        }

        try:
            self.producer.send(KAFKA_TOPICS["fraud_results"], value=result)
        except KafkaError as e:
            logger.error(f"Result publish hatası: {e}")

        # === DiCE — only if fraud, compute asynchronously ===
        if prediction["is_fraud"]:
            self._dice_executor.submit(
                self._compute_and_publish_counterfactual,
                transaction,
                prediction,
                result["transaction_id"],
            )

        # === 5. Prometheus Metrics ===
        pipeline_time = time.perf_counter() - t_pipeline_start
        PIPELINE_LATENCY.observe(pipeline_time)
        TRANSACTIONS_TOTAL.labels(risk_level=prediction["risk_level"]).inc()

        if prediction["is_fraud"]:
            FRAUD_DETECTED.inc()

        self._processed += 1

        # Compute TPS
        elapsed = time.time() - self._start_time
        if elapsed > 0:
            THROUGHPUT_GAUGE.set(self._processed / elapsed)

        # Log (when fraud is detected or every 100 transactions)
        if prediction["is_fraud"]:
            logger.warning(
                f"🔴 FRAUD | prob={prediction['fraud_probability']:.4f} "
                f"risk={prediction['risk_level']} "
                f"amt={transaction.get('amt', 0):.2f} "
                f"latency={pipeline_time*1000:.1f}ms "
                f"| top_shap: {prediction['shap_explanation'][0]['feature'] if prediction['shap_explanation'] else 'N/A'}"
            )
        elif self._processed % 100 == 0:
            tps = self._processed / elapsed if elapsed > 0 else 0
            logger.info(
                f"  İşlendi: {self._processed:,} | "
                f"TPS: {tps:,.0f} | "
                f"p_latency: {pipeline_time*1000:.1f}ms"
            )
        
    def _compute_and_publish_counterfactual(
        self, transaction: dict, prediction: dict, txn_id: str
    ):
        """
        Compute the DiCE counterfactual explanation asynchronously and
        send it to the fraud-results topic as a patch.
        """
        try:
            t0 = time.perf_counter()
            cf = self.engine.explain_counterfactual(transaction, prediction)
            elapsed = (time.perf_counter() - t0) * 1000

            if cf:
                patch = {
                    "transaction_id": txn_id,
                    "counterfactual": cf,
                    "counterfactual_latency_ms": round(elapsed, 2),
                    "type": "counterfactual_patch",
                }
                self.producer.send(KAFKA_TOPICS["fraud_results"], value=patch)
                logger.info(
                    f"  🔬 DiCE tamamlandı: {txn_id} ({elapsed:.0f}ms)"
                )
        except Exception as e:
            logger.error(f"DiCE hatası ({txn_id}): {e}")

    def _publish_drift_alert(self, alert: dict):
        """Publish the drift alert to the Kafka topic."""
        try:
            self.producer.send(KAFKA_TOPICS["drift_alerts"], value=alert)
            DRIFT_ALERTS.labels(severity=alert["overall_status"]).inc()
            logger.warning(
                f"⚠️  DRIFT ALERT yayınlandı: {alert['overall_status']} "
                f"({alert['metrics']['n_critical_features']} critical features)"
            )
        except KafkaError as e:
            logger.error(f"Drift alert publish hatası: {e}")

    def process_analyst_feedback(self, feedback: dict):
        """
        Process analyst feedback (feedback loop).
        Read from the analyst-feedback topic.

        Feedback format:
        {
            "transaction_id": "...",
            "analyst_label": true/false,  # is it real fraud?
            "analyst_id": "...",
            "timestamp": "..."
        }

        These feedbacks are accumulated and trigger periodic retraining.
        """
        logger.info(
            f"Feedback alındı: tx={feedback.get('transaction_id')} "
            f"label={feedback.get('analyst_label')}"
        )
        # TODO: Accumulate the feedbacks and, once enough have been reached,
        # trigger the retrain pipeline (MLflow integration)

    def _shutdown(self):
        """Clean shutdown."""
        logger.info("\nPipeline kapatılıyor...")

        self._dice_executor.shutdown(wait=False)  # ← added

        if self.consumer:
            self.consumer.close()
            logger.info("  ✓ Kafka consumer kapatıldı")

        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("  ✓ Kafka producer kapatıldı")

        self.neo4j.close()

        elapsed = time.time() - self._start_time if self._start_time else 0
        tps = self._processed / elapsed if elapsed > 0 else 0

        logger.info(f"\n  Toplam işlenen: {self._processed:,}")
        logger.info(f"  Çalışma süresi: {elapsed:.1f}s")
        logger.info(f"  Ortalama TPS:   {tps:,.0f}")
        logger.info("Pipeline durduruldu.")


# =============================================================================
# Entry Point
# =============================================================================
def main():
    pipeline = FraudDetectionConsumer()
    pipeline.initialize()
    pipeline.run()


if __name__ == "__main__":
    main()
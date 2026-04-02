"""
Merkezi Konfigürasyon — Kafka Fraud Detection Pipeline
======================================================
Tüm servisler (producer, consumer, neo4j, drift) bu dosyadan okur.
Ortam değişkenleri ile override edilebilir.
"""

import os

# =============================================================================
# Kafka
# =============================================================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

KAFKA_TOPICS = {
    "raw_transactions": "raw-transactions",
    "fraud_results":    "fraud-results",
    "drift_alerts":     "drift-alerts",
    "analyst_feedback": "analyst-feedback",
}

KAFKA_CONSUMER_GROUP = "fraud-detection-pipeline"

# =============================================================================
# Model Paths
# =============================================================================
_BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
MODEL_DIR = os.getenv("MODEL_DIR", os.path.join(_BASE_DIR, "models"))

LGBM_MODEL_PATH          = os.path.join(MODEL_DIR, "lgbm_model.pkl")
LGBM_CONTRACT_PATH        = os.path.join(MODEL_DIR, "lgbm_feature_contract.json")
AE_MODEL_PATH             = os.path.join(MODEL_DIR, "autoencoder_model.keras")
AE_SCALER_PATH            = os.path.join(MODEL_DIR, "ae_scaler.pkl")
REFERENCE_DIST_PATH       = os.path.join(MODEL_DIR, "reference_distributions.json")

# =============================================================================
# Feature Contract (train_lightgbm.ipynb ile aynı)
# =============================================================================
LGBM_FEATURES = [
    "amt", "age", "city_pop", "geo_distance_km", "geo_distance_log",
    "time_since_last_tx", "tx_count_1h", "amt_sum_24h", "amt_mean_hist",
    "amt_vs_mean", "hour", "day_of_week", "month", "is_weekend", "is_night",
    "is_online", "is_pos", "category", "gender", "ae_anomaly_score",
]

# AE modeli bu 14 feature'ı kullanır (train_ae.ipynb'den)
AE_FEATURES = [
    "amt", "age", "city_pop", "geo_distance_km", "geo_distance_log",
    "time_since_last_tx", "tx_count_1h", "amt_sum_24h", "amt_mean_hist",
    "amt_vs_mean", "hour", "day_of_week", "month", "ae_anomaly_score",
]

CATEGORICAL_FEATURES = ["category", "gender"]
BINARY_FEATURES      = ["is_weekend", "is_night", "is_online", "is_pos"]

# =============================================================================
# Drift Monitoring
# =============================================================================
PSI_THRESHOLDS = {
    "stable":   0.1,
    "warning":  0.2,
    "critical": 0.25,
}

KL_THRESHOLDS = {
    "stable":   0.05,
    "warning":  0.1,
    "critical": 0.2,
}

DRIFT_WINDOW_SIZE     = 5000   # Kaç işlem birikmeli ki drift kontrol edilsin
DRIFT_CHECK_INTERVAL  = 5000   # Her N işlemde bir drift kontrolü

# =============================================================================
# Neo4j
# =============================================================================
NEO4J_URI      = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER     = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "fraud-detection-2024")

# =============================================================================
# Performance Targets (tez önerisinden)
# =============================================================================
TARGET_P95_LATENCY_MS = 200    # p95 ≤ 200ms
TARGET_THROUGHPUT_TPS = 1000   # ≥ 1000 TPS

# =============================================================================
# Producer
# =============================================================================
PRODUCER_DATA_PATH = os.getenv(
    "PRODUCER_DATA_PATH",
    "./data/test_final_2020_with_ae_scores.csv"
)
PRODUCER_RATE_TPS  = int(os.getenv("PRODUCER_RATE_TPS", "100"))

# =============================================================================
# Prometheus
# =============================================================================
PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", "8000"))

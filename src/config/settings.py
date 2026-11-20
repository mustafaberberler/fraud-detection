"""
Central Configuration — Kafka Fraud Detection Pipeline
======================================================
All services (producer, consumer, neo4j, drift) read from this file.
Can be overridden with environment variables.
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
AE_MODEL_PATH             = os.path.join(MODEL_DIR, "autoencoder.keras")
AE_SCALER_PATH            = os.path.join(MODEL_DIR, "ae_scaler.pkl")
REFERENCE_DIST_PATH       = os.path.join(MODEL_DIR, "reference_distributions.json")

# Reference data for the DiCE counterfactual explainer (first 500 rows are used).
# Defaults to <repo>/data/...; override with the DICE_REFERENCE_DATA_PATH env var.
DATA_DIR = os.getenv("DATA_DIR", os.path.join(_BASE_DIR, "data"))
DICE_REFERENCE_DATA_PATH = os.getenv(
    "DICE_REFERENCE_DATA_PATH",
    os.path.join(DATA_DIR, "train_2019_with_ae_scores.csv"),
)

# =============================================================================
# Feature Contract (same as train_lightgbm.ipynb)
# =============================================================================
LGBM_FEATURES = [
    "amt", "age", "city_pop", "geo_distance_km", "geo_distance_log",
    "time_since_last_tx", "tx_count_1h", "amt_sum_24h", "amt_mean_hist",
    "amt_vs_mean", "hour", "day_of_week", "month", "is_weekend", "is_night",
    "is_online", "is_pos", "category", "gender", "ae_anomaly_score",
]

# The AE model uses these 14 features, in this exact order (from train_ae.ipynb).
# The order must match the fitted ae_scaler.pkl (RobustScaler.feature_names_in_).
AE_FEATURES = [
    "amt", "hour", "day_of_week", "is_weekend", "is_night", "age",
    "geo_distance_log", "city_pop", "time_since_last_tx", "tx_count_1h",
    "amt_sum_24h", "amt_vs_mean", "is_online", "is_pos",
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

DRIFT_WINDOW_SIZE     = 5000   # How many transactions must accumulate before drift is checked
DRIFT_CHECK_INTERVAL  = 5000   # Check drift once every N transactions

# =============================================================================
# Neo4j
# =============================================================================
NEO4J_URI      = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER     = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "fraud-detection-2024")

# =============================================================================
# Performance Targets (from the thesis proposal)
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
##PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", "8000"))
PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", "8099"))

# Real-Time Fraud Detection Pipeline

**Thesis:** Real-Time, Explainable Hybrid AI for Financial Fraud Detection  
**Advisor:** Assoc. Prof. Dr. Özgür GÜMÜŞ — Ege University

> **Türkçe:** [README.tr.md](README.tr.md)

A streaming fraud detection system: an Autoencoder anomaly score feeds a LightGBM classifier, every prediction is explained with TreeSHAP (and DiCE counterfactuals), transactions are written to a Neo4j graph for PageRank / community analysis, and input drift is monitored with PSI and KL divergence.

| Repository | Purpose |
|---|---|
| **fraud-detection** (this repo) | Pipeline: Kafka producer/consumer, ML inference, drift monitoring, Neo4j graph, notebooks |
| [fraud-detection-dashboard](https://github.com/mustafaberberler/fraud-detection-dashboard) | Web dashboard (React + FastAPI) that visualizes this pipeline's output |
| Data: [10.5281/zenodo.24404379](https://doi.org/10.5281/zenodo.24404379) | Train / test CSV files (not stored in Git, see [data/README.md](data/README.md)) |

## Architecture

```
Sparkov CSV ──→ [Producer] ──→ Kafka: raw-transactions
                                        │
                                        ▼
                              ┌─────────────────────────────┐
                              │      Consumer Pipeline      │
                              │                             │
                              │  1. Autoencoder → ae_score  │
                              │  2. LightGBM   → fraud_prob │
                              │  3. TreeSHAP   → explanation│
                              │  4. Neo4j      → graph write│
                              │  5. Drift      → PSI/KL     │
                              │  6. Prometheus → metrics    │
                              └──────────┬──────────────────┘
                                         │
                          ┌──────────────┼──────────────┐
                          ▼              ▼              ▼
                   fraud-results   drift-alerts   Neo4j Graph
                          │              │              │
                          ▼              ▼              ▼
                  Dashboard repo      Grafana       PageRank +
                                                   Fraud Rings
```

## Requirements

- Python **3.11**
- Docker with Docker Compose (Kafka, Neo4j + GDS, Prometheus, Grafana)
- Free disk space for the Docker images and the data (≈ 0.5 GB unzipped)
- Neo4j is configured with a 2 GB heap and a 1 GB page cache (see `docker-compose.yml`)

## Quick start

All commands are run from the repository root unless stated otherwise.

### 1. Install the Python dependencies

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

`tensorflow` (needed only to compute the Autoencoder score live) is optional and commented out in `requirements.txt` — see [Optional components](#optional-components).

### 2. Get the data

The CSV files are hosted on Zenodo ([10.5281/zenodo.24404379](https://doi.org/10.5281/zenodo.24404379)). Download `data.zip` and extract it into `data/`:

```bash
unzip data.zip -d data/
```

Details, file descriptions and checksums: [data/README.md](data/README.md).

### 3. Start the infrastructure

```bash
docker-compose up -d        # or: docker compose up -d
```

| Service | URL | Credentials |
|---|---|---|
| Kafka | `localhost:9092` | — |
| Neo4j Browser | http://localhost:7474 | `neo4j` / `fraud-detection-2024` |
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | `admin` / `fraud2024` (anonymous read-only access is also enabled) |

The four Kafka topics are created automatically by the `kafka-init` container. Wait until Kafka and Neo4j report healthy (`docker-compose ps`).

### 4. Models

The trained models are already included in [models/](models/) (`lgbm_model.pkl`, `lgbm_feature_contract.json`, `reference_distributions.json`, `ae_scaler.pkl`, `autoencoder.keras`) — nothing to copy. To retrain them, see [Reproducing the models](#reproducing-the-models).

### 5. Start the consumer

```bash
cd src
python -m consumer.consumer
```

Prometheus metrics are served on http://localhost:8099/metrics (change with `PROMETHEUS_PORT`).

### 6. Start the producer (second terminal)

```bash
cd src
python -m producer.producer --tps 100 --data ../data/test_final_2020_with_ae_scores.csv
```

Useful options: `--limit N` (send only N records), `--loop` (restart when the file ends).

### 7. (Optional) Start the dashboard

The web dashboard lives in its own repository: [fraud-detection-dashboard](https://github.com/mustafaberberler/fraud-detection-dashboard). Clone it **next to this repo**, then follow its README. Start order: infrastructure → consumer → producer → dashboard API → dashboard frontend.

## Reproducing the models

The notebooks in `src/ml/` were run on Google Colab (paths such as `/content/drive/MyDrive/thesis/...` at the top of each notebook need adjusting for local runs). Run them in this order:

| # | Notebook | Produces |
|---|---|---|
| 1 | `train_ae.ipynb` | Autoencoder (`autoencoder.keras`), `ae_scaler.pkl`, `*_with_ae_scores.csv` (the Zenodo data) |
| 2 | `train_lightgbm.ipynb` | `lgbm_model.pkl`, `lgbm_feature_contract.json`, `training_report.json`, SHAP / DiCE analyses, model comparison |
| 3 | `drift.ipynb` | `reference_distributions.json`, `drift_state.json`, drift analysis and adaptive threshold calibration |

The feature lists in `src/config/settings.py` (`LGBM_FEATURES`, `AE_FEATURES`) must match the notebooks exactly.

## Optional components

- **Autoencoder at inference time.** The data files already contain `ae_anomaly_score`, so the consumer uses it and the Autoencoder is *not* needed to replay the provided data. To compute the score live (e.g. for data without the column), uncomment `tensorflow` in `requirements.txt`; the consumer then loads `models/autoencoder.keras` automatically.
- **DiCE counterfactual explanations** (`dice-ml`) run asynchronously for flagged transactions. They need reference data: the first 500 rows of `data/train_2019_with_ae_scores.csv` (path configurable with `DICE_REFERENCE_DATA_PATH`). If the file is missing the consumer logs a warning and keeps running without counterfactuals.

## Configuration

Defaults are in [src/config/settings.py](src/config/settings.py); every value below can be overridden with an environment variable.

| Variable | Default | Meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker |
| `MODEL_DIR` | `<repo>/models` | Folder with the trained models |
| `DATA_DIR` | `<repo>/data` | Folder with the CSV data |
| `DICE_REFERENCE_DATA_PATH` | `<DATA_DIR>/train_2019_with_ae_scores.csv` | Reference data for DiCE |
| `PRODUCER_DATA_PATH` | `./data/test_final_2020_with_ae_scores.csv` | Default input of the producer (relative to the working directory; `--data` overrides it) |
| `PRODUCER_RATE_TPS` | `100` | Default producer rate |
| `NEO4J_URI` / `NEO4J_USER` / `NEO4J_PASSWORD` | `bolt://localhost:7687` / `neo4j` / `fraud-detection-2024` | Neo4j connection |
| `PROMETHEUS_PORT` | `8099` | Port of the consumer's metrics endpoint |

## Project structure

```
├── docker-compose.yml          # Kafka + Neo4j (GDS) + Prometheus + Grafana
├── prometheus.yml              # Prometheus scrape configuration
├── requirements.txt            # Python dependencies
├── grafana/                    # Grafana dashboard + provisioning
├── models/                     # Trained model artifacts
├── data/                       # CSV data (downloaded from Zenodo, not in Git)
├── results/                    # SHAP / XAI figures and JSON outputs
└── src/
    ├── config/settings.py      # Central configuration
    ├── producer/producer.py    # Sparkov CSV → Kafka streamer
    ├── consumer/consumer.py    # Main pipeline (AE→LightGBM→SHAP→Neo4j→Drift)
    ├── ml/
    │   ├── inference.py        # Model wrapper (AE + LightGBM + SHAP + DiCE)
    │   ├── drift_monitor.py    # Real-time PSI/KL checker
    │   ├── train_ae.ipynb      # Autoencoder training
    │   ├── train_lightgbm.ipynb# LightGBM training, XAI, experiments
    │   └── drift.ipynb         # Drift analysis and calibration
    └── graph/
        └── neo4j_client.py     # Neo4j writes + PageRank + community / fraud-ring detection
```

## Kafka topics

| Topic | Direction | Content |
|---|---|---|
| `raw-transactions` | Producer → Consumer | Raw Sparkov transactions (feature-engineered) |
| `fraud-results` | Consumer → Dashboard | Prediction + SHAP + latency (+ DiCE counterfactual patch) |
| `drift-alerts` | Consumer → (Grafana / consumers) | PSI/KL drift alerts |
| `analyst-feedback` | Dashboard → Consumer | Analyst feedback (consumer handler is a stub) |

## Prometheus metrics

| Metric | Type | Description |
|---|---|---|
| `fraud_transactions_total` | Counter | Total processed transactions (labelled by `risk_level`) |
| `fraud_detected_total` | Counter | Number of detected frauds |
| `pipeline_latency_seconds` | Histogram | Full pipeline latency |
| `inference_latency_seconds` | Histogram | ML inference latency |
| `neo4j_write_latency_seconds` | Histogram | Neo4j write duration |
| `drift_alerts_total` | Counter | Number of drift alerts (labelled by `severity`) |
| `pipeline_throughput_tps` | Gauge | Current throughput |

## Neo4j graph schema

```
(Customer)-[:MADE]->(Transaction)-[:AT]->(Merchant)
```

Graph analytics (Neo4j Graph Data Science, with a plain-Cypher fallback if GDS is unavailable):

- **PageRank**: riskiest customers, weighted by fraud probability on the edges
- **Louvain community detection**: fraud-ring candidate communities ranked by fraud-rate lift
- **Fraud-ring pairs**: customer pairs that committed fraud at the same merchants
- **Customer graph features**: graph-based risk signals per customer

## Known limitations

- `src/graph/bootstrap_constraints.py`, `bootstrap_m1.py` and `bootstrap_m2.py` are leftovers from an earlier graph-metamodel experiment. They import a module (`src.graph.neo4j_driver`) that is not part of this repository and are **not used** by the pipeline.
- The consumer's real-time drift alerts are published to Kafka (`drift-alerts`); the dashboard's drift page currently shows a static snapshot generated by `drift.ipynb` (see the dashboard README).
- The notebooks were developed on Colab; local paths must be adjusted.

## Troubleshooting

- **`ModuleNotFoundError: config`** — run the consumer/producer from inside `src/` (`cd src`).
- **DiCE reference data warning** — extract the data into `data/` (step 2) or set `DICE_REFERENCE_DATA_PATH`.
- **Neo4j warnings** — wait until the Neo4j container is healthy; the pipeline keeps running without the graph if Neo4j is unreachable.
- **Prometheus shows no `fraud-pipeline` target** — the consumer must be running on the host; the Prometheus container reaches it through `host.docker.internal:8099`.

<!-- TODO: add the paper citation (BibTeX) here once available -->
## Citation

If you use this code or data, please cite the accompanying paper and the dataset (Zenodo DOI: [10.5281/zenodo.24404379](https://doi.org/10.5281/zenodo.24404379)).

The transactions originate from the synthetic Sparkov dataset ([Kaggle: kartik2112/fraud-detection](https://www.kaggle.com/datasets/kartik2112/fraud-detection)).

## License

MIT — see [LICENSE](LICENSE).

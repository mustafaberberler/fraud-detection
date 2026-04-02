# Gerçek Zamanlı Fraud Detection Pipeline

**Tez:** Gerçek Zamanlı, Açıklanabilir Hibrit AI ile Finansal Sahtekârlık Tespiti  
**Danışman:** Doç. Dr. Özgür GÜMÜŞ — Ege Üniversitesi

## Mimari

```
Sparkov CSV ──→ [Producer] ──→ Kafka: raw-transactions
                                        │
                                        ▼
                              ┌─────────────────────────────┐
                              │      Consumer Pipeline      │
                              │                             │
                              │  1. Autoencoder → ae_score  │
                              │  2. LightGBM   → fraud_prob │
                              │  3. TreeSHAP   → açıklama   │
                              │  4. Neo4j      → graf yazma  │
                              │  5. Drift      → PSI/KL     │
                              │  6. Prometheus → metrikler   │
                              └──────────┬──────────────────┘
                                         │
                          ┌──────────────┼──────────────┐
                          ▼              ▼              ▼
                   fraud-results   drift-alerts   Neo4j Graph
                          │              │              │
                          ▼              ▼              ▼
                       Grafana      Grafana      PageRank +
                      Dashboard    Alarmlar    Fraud Ring
```

## Hızlı Başlangıç

### 1. Altyapıyı Başlat

```bash
docker-compose up -d
```

Servisler:

- Kafka: `localhost:9092`
- Neo4j Browser: `http://localhost:7474` (neo4j / fraud-detection-2024)
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (admin / fraud2024)

### 2. Model Dosyalarını Yerleştir

```bash
mkdir -p models/
# Colab'dan indirilen dosyalar:
cp lgbm_model.pkl lgbm_feature_contract.json reference_distributions.json models/
# (opsiyonel) Autoencoder:
cp autoencoder_model.keras ae_scaler.pkl models/
```

### 3. Consumer'ı Başlat

```bash
cd src/
python -m consumer.consumer
```

### 4. Producer'ı Başlat

```bash
cd src/
python -m producer.producer --tps 100 --data ../data/test_final_2020_with_ae_scores.csv
```

## Dosya Yapısı

```
├── docker-compose.yml          # Kafka + Neo4j + Prometheus + Grafana
├── prometheus.yml              # Prometheus scrape konfigürasyonu
├── requirements.txt            # Python bağımlılıkları
├── models/                     # Eğitilmiş model artefaktları
│   ├── lgbm_model.pkl
│   ├── lgbm_feature_contract.json
│   └── reference_distributions.json
├── data/                       # Sparkov CSV dosyaları
│   └── test_final_2020_with_ae_scores.csv
└── src/
    ├── config/
    │   └── settings.py         # Merkezi konfigürasyon
    ├── producer/
    │   └── producer.py         # Sparkov CSV → Kafka streamer
    ├── consumer/
    │   └── consumer.py         # Ana pipeline (AE→LightGBM→SHAP→Neo4j→Drift)
    ├── ml/
    │   ├── inference.py        # Model wrapper (AE + LightGBM + SHAP)
    │   └── drift_monitor.py    # Gerçek zamanlı PSI/KL checker
    └── graph/
        └── neo4j_client.py     # Neo4j CRUD + PageRank + Fraud Ring
```

## Kafka Topics

| Topic              | Yön                  | İçerik                             |
| ------------------ | -------------------- | ---------------------------------- |
| `raw-transactions` | Producer → Consumer  | Ham Sparkov işlemleri (20 feature) |
| `fraud-results`    | Consumer → Dashboard | Tahmin + SHAP + latency            |
| `drift-alerts`     | Consumer → Grafana   | PSI/KL drift alarmları             |
| `analyst-feedback` | Dashboard → Consumer | Analist geri bildirimleri          |

## Prometheus Metrikleri

| Metrik                        | Tip       | Açıklama                                   |
| ----------------------------- | --------- | ------------------------------------------ |
| `fraud_transactions_total`    | Counter   | İşlenen toplam işlem (risk_level etiketli) |
| `fraud_detected_total`        | Counter   | Tespit edilen fraud sayısı                 |
| `pipeline_latency_seconds`    | Histogram | Tam pipeline latency                       |
| `inference_latency_seconds`   | Histogram | ML inference süresi                        |
| `neo4j_write_latency_seconds` | Histogram | Neo4j yazma süresi                         |
| `drift_alerts_total`          | Counter   | Drift alarm sayısı                         |
| `pipeline_throughput_tps`     | Gauge     | Anlık throughput                           |

## Neo4j Graph Şeması

```
(Customer)-[:MADE]->(Transaction)-[:AT]->(Merchant)
```

Graph Analytics:

- **PageRank**: En riskli müşterileri bul (fraud bağlantı ağırlığı)
- **Fraud Ring Detection**: Aynı merchant'larda fraud yapan müşteri kümeleri
- **Customer Graph Features**: Her müşteri için graph-based risk sinyalleri

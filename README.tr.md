# Gerçek Zamanlı Fraud Detection Pipeline

**Tez:** Gerçek Zamanlı, Açıklanabilir Hibrit AI ile Finansal Sahtekârlık Tespiti  
**Danışman:** Doç. Dr. Özgür GÜMÜŞ — Ege Üniversitesi

> **English:** [README.md](README.md)

Akış tabanlı bir sahtekârlık tespit sistemi: Autoencoder anomaly skoru LightGBM sınıflandırıcısına girdi olur, her tahmin TreeSHAP (ve DiCE counterfactual) ile açıklanır, işlemler PageRank / topluluk analizi için Neo4j grafına yazılır ve girdi dağılımındaki kayma (drift) PSI ve KL divergence ile izlenir.

| Repo | Amaç |
|---|---|
| **fraud-detection** (bu repo) | Hat: Kafka producer/consumer, ML inference, drift izleme, Neo4j graf, notebook'lar |
| [fraud-detection-dashboard](https://github.com/mustafaberberler/fraud-detection-dashboard) | Bu hattın çıktısını görselleştiren web paneli (React + FastAPI) |
| Veri: [10.5281/zenodo.24404379](https://doi.org/10.5281/zenodo.24404379) | Train / test CSV dosyaları (Git'te tutulmaz, bkz. [data/README.md](data/README.md)) |

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
                              │  4. Neo4j      → graf yazma │
                              │  5. Drift      → PSI/KL     │
                              │  6. Prometheus → metrikler  │
                              └──────────┬──────────────────┘
                                         │
                          ┌──────────────┼──────────────┐
                          ▼              ▼              ▼
                   fraud-results   drift-alerts   Neo4j Graph
                          │              │              │
                          ▼              ▼              ▼
                   Dashboard repo      Grafana       PageRank +
                                                    Fraud Ring
```

## Gereksinimler

- Python **3.11**
- Docker ve Docker Compose (Kafka, Neo4j + GDS, Prometheus, Grafana)
- Docker imajları ve veri için boş disk alanı (veri açılmış hâliyle ≈ 0,5 GB)
- Neo4j 2 GB heap ve 1 GB page cache ile yapılandırılmıştır (bkz. `docker-compose.yml`)

## Hızlı başlangıç

Aksi belirtilmedikçe tüm komutlar repo kök klasöründen çalıştırılır.

### 1. Python bağımlılıklarını kur

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

`tensorflow` (yalnızca Autoencoder skorunu canlı hesaplamak için gerekir) opsiyoneldir ve `requirements.txt` içinde yorum satırıdır — bkz. [Opsiyonel bileşenler](#opsiyonel-bileşenler).

### 2. Veriyi indir

CSV dosyaları Zenodo'da barındırılıyor ([10.5281/zenodo.24404379](https://doi.org/10.5281/zenodo.24404379)). `data.zip` dosyasını indirip `data/` içine aç:

```bash
unzip data.zip -d data/
```

Ayrıntılar, dosya açıklamaları ve checksum'lar: [data/README.md](data/README.md).

### 3. Altyapıyı başlat

```bash
docker-compose up -d        # veya: docker compose up -d
```

| Servis | URL | Kimlik bilgisi |
|---|---|---|
| Kafka | `localhost:9092` | — |
| Neo4j Browser | http://localhost:7474 | `neo4j` / `fraud-detection-2024` |
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | `admin` / `fraud2024` (anonim salt-okunur erişim de açık) |

Dört Kafka topic'i `kafka-init` container'ı tarafından otomatik oluşturulur. Kafka ve Neo4j "healthy" olana kadar bekle (`docker-compose ps`).

### 4. Modeller

Eğitilmiş modeller [models/](models/) klasöründe hazır (`lgbm_model.pkl`, `lgbm_feature_contract.json`, `reference_distributions.json`, `ae_scaler.pkl`, `autoencoder.keras`) — kopyalanacak bir şey yok. Yeniden eğitmek için bkz. [Modelleri yeniden üretme](#modelleri-yeniden-üretme).

### 5. Consumer'ı başlat

```bash
cd src
python -m consumer.consumer
```

Prometheus metrikleri http://localhost:8099/metrics adresinden sunulur (`PROMETHEUS_PORT` ile değiştirilebilir).

### 6. Producer'ı başlat (ikinci terminal)

```bash
cd src
python -m producer.producer --tps 100 --data ../data/test_final_2020_with_ae_scores.csv
```

Faydalı seçenekler: `--limit N` (yalnızca N kayıt gönder), `--loop` (dosya bitince başa sar).

### 7. (Opsiyonel) Paneli başlat

Web paneli kendi reposunda: [fraud-detection-dashboard](https://github.com/mustafaberberler/fraud-detection-dashboard). Onu **bu reponun yanına** klonla ve kendi README'sini izle. Başlatma sırası: altyapı → consumer → producer → panel API'si → panel frontend'i.

## Modelleri yeniden üretme

`src/ml/` içindeki notebook'lar Google Colab'da çalıştırıldı (her notebook'un başındaki `/content/drive/MyDrive/thesis/...` gibi yollar yerel çalıştırma için düzenlenmelidir). Şu sırayla çalıştır:

| # | Notebook | Ürettikleri |
|---|---|---|
| 1 | `train_ae.ipynb` | Autoencoder (`autoencoder.keras`), `ae_scaler.pkl`, `*_with_ae_scores.csv` (Zenodo'daki veri) |
| 2 | `train_lightgbm.ipynb` | `lgbm_model.pkl`, `lgbm_feature_contract.json`, `training_report.json`, SHAP / DiCE analizleri, model karşılaştırması |
| 3 | `drift.ipynb` | `reference_distributions.json`, `drift_state.json`, drift analizi ve adaptif eşik kalibrasyonu |

`src/config/settings.py` içindeki feature listeleri (`LGBM_FEATURES`, `AE_FEATURES`) notebook'larla birebir aynı olmalıdır.

## Opsiyonel bileşenler

- **Inference sırasında Autoencoder.** Veri dosyalarında `ae_anomaly_score` zaten var; consumer onu kullanır, yani sağlanan veriyi oynatmak için Autoencoder'a *gerek yoktur*. Skoru canlı hesaplamak için (ör. bu sütunu içermeyen veri) `requirements.txt` içindeki `tensorflow` satırının yorumunu kaldır; consumer `models/autoencoder.keras` dosyasını otomatik yükler.
- **DiCE counterfactual açıklamaları** (`dice-ml`) işaretlenen işlemler için asenkron çalışır. Referans veri gerekir: `data/train_2019_with_ae_scores.csv` dosyasının ilk 500 satırı (yol `DICE_REFERENCE_DATA_PATH` ile ayarlanabilir). Dosya yoksa consumer uyarı loglar ve counterfactual olmadan çalışmaya devam eder.

## Yapılandırma

Varsayılanlar [src/config/settings.py](src/config/settings.py) içindedir; aşağıdaki her değer ortam değişkeniyle değiştirilebilir.

| Değişken | Varsayılan | Anlamı |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker |
| `MODEL_DIR` | `<repo>/models` | Eğitilmiş modellerin klasörü |
| `DATA_DIR` | `<repo>/data` | CSV verisinin klasörü |
| `DICE_REFERENCE_DATA_PATH` | `<DATA_DIR>/train_2019_with_ae_scores.csv` | DiCE referans verisi |
| `PRODUCER_DATA_PATH` | `./data/test_final_2020_with_ae_scores.csv` | Producer'ın varsayılan girdisi (çalışma klasörüne göre; `--data` ezer) |
| `PRODUCER_RATE_TPS` | `100` | Varsayılan producer hızı |
| `NEO4J_URI` / `NEO4J_USER` / `NEO4J_PASSWORD` | `bolt://localhost:7687` / `neo4j` / `fraud-detection-2024` | Neo4j bağlantısı |
| `PROMETHEUS_PORT` | `8099` | Consumer metrik uç noktasının portu |

## Proje yapısı

```
├── docker-compose.yml          # Kafka + Neo4j (GDS) + Prometheus + Grafana
├── prometheus.yml              # Prometheus scrape yapılandırması
├── requirements.txt            # Python bağımlılıkları
├── grafana/                    # Grafana paneli + provisioning
├── models/                     # Eğitilmiş model artefaktları
├── data/                       # CSV verisi (Zenodo'dan indirilir, Git'te yok)
├── results/                    # SHAP / XAI görselleri ve JSON çıktıları
└── src/
    ├── config/settings.py      # Merkezi yapılandırma
    ├── producer/producer.py    # Sparkov CSV → Kafka streamer
    ├── consumer/consumer.py    # Ana pipeline (AE→LightGBM→SHAP→Neo4j→Drift)
    ├── ml/
    │   ├── inference.py        # Model wrapper (AE + LightGBM + SHAP + DiCE)
    │   ├── drift_monitor.py    # Gerçek zamanlı PSI/KL denetleyicisi
    │   ├── train_ae.ipynb      # Autoencoder eğitimi
    │   ├── train_lightgbm.ipynb# LightGBM eğitimi, XAI, deneyler
    │   └── drift.ipynb         # Drift analizi ve kalibrasyon
    └── graph/
        └── neo4j_client.py     # Neo4j yazma + PageRank + topluluk / fraud ring tespiti
```

## Kafka topic'leri

| Topic | Yön | İçerik |
|---|---|---|
| `raw-transactions` | Producer → Consumer | Ham Sparkov işlemleri (feature-engineered) |
| `fraud-results` | Consumer → Dashboard | Tahmin + SHAP + latency (+ DiCE counterfactual yaması) |
| `drift-alerts` | Consumer → (Grafana / tüketiciler) | PSI/KL drift alarmları |
| `analyst-feedback` | Dashboard → Consumer | Analist geri bildirimleri (consumer tarafındaki işleyici taslak) |

## Prometheus metrikleri

| Metrik | Tip | Açıklama |
|---|---|---|
| `fraud_transactions_total` | Counter | İşlenen toplam işlem (`risk_level` etiketli) |
| `fraud_detected_total` | Counter | Tespit edilen fraud sayısı |
| `pipeline_latency_seconds` | Histogram | Tam pipeline latency |
| `inference_latency_seconds` | Histogram | ML inference süresi |
| `neo4j_write_latency_seconds` | Histogram | Neo4j yazma süresi |
| `drift_alerts_total` | Counter | Drift alarm sayısı (`severity` etiketli) |
| `pipeline_throughput_tps` | Gauge | Anlık throughput |

## Neo4j graf şeması

```
(Customer)-[:MADE]->(Transaction)-[:AT]->(Merchant)
```

Graf analitiği (Neo4j Graph Data Science; GDS yoksa düz Cypher'a geri düşer):

- **PageRank**: kenarlardaki fraud olasılığıyla ağırlıklandırılmış en riskli müşteriler
- **Louvain topluluk tespiti**: fraud oranı "lift" değerine göre sıralanan fraud ring aday toplulukları
- **Fraud ring çiftleri**: aynı merchant'larda fraud yapan müşteri çiftleri
- **Müşteri graf feature'ları**: her müşteri için graf tabanlı risk sinyalleri

## Bilinen sınırlamalar

- `src/graph/bootstrap_constraints.py`, `bootstrap_m1.py` ve `bootstrap_m2.py`, önceki bir graf metamodel denemesinden kalmadır. Bu repoda bulunmayan bir modülü (`src.graph.neo4j_driver`) import ederler ve hat tarafından **kullanılmazlar**.
- Consumer'ın gerçek zamanlı drift alarmları Kafka'ya (`drift-alerts`) yayınlanır; panelin drift sayfası şu an `drift.ipynb`'nin ürettiği statik bir anlık görüntüyü gösterir (bkz. panel README'si).
- Notebook'lar Colab'da geliştirilmiştir; yerel yollar düzenlenmelidir.

## Sorun giderme

- **`ModuleNotFoundError: config`** — consumer/producer'ı `src/` içinden çalıştır (`cd src`).
- **DiCE referans verisi uyarısı** — veriyi `data/` içine aç (adım 2) veya `DICE_REFERENCE_DATA_PATH` ayarla.
- **Neo4j uyarıları** — Neo4j container'ı "healthy" olana kadar bekle; Neo4j'ye ulaşılamazsa hat graf olmadan çalışmaya devam eder.
- **Prometheus'ta `fraud-pipeline` hedefi görünmüyor** — consumer host üzerinde çalışıyor olmalı; Prometheus container'ı ona `host.docker.internal:8099` üzerinden ulaşır.

<!-- TODO: makale atfı (BibTeX) hazır olunca buraya ekle -->
## Atıf

Bu kodu veya veriyi kullanırsanız lütfen ilgili makaleye ve veri setine atıf yapın (Zenodo DOI: [10.5281/zenodo.24404379](https://doi.org/10.5281/zenodo.24404379)).

İşlemler sentetik Sparkov veri setinden gelir ([Kaggle: kartik2112/fraud-detection](https://www.kaggle.com/datasets/kartik2112/fraud-detection)).

## Lisans

MIT — bkz. [LICENSE](LICENSE).

# Data

The CSV files used by this project are too large for GitHub, so they are hosted on Zenodo:

**DOI: [10.5281/zenodo.24404379](https://doi.org/10.5281/zenodo.24404379)**

## Files

| File | Rows | Columns | Period | Fraud rate | Used by |
|---|---|---|---|---|---|
| `train_2019_with_ae_scores.csv` | 924,850 | 38 | 2019-01-01 → 2019-12-31 | 0.564% (5,220 frauds) | LightGBM training, drift reference, DiCE reference data |
| `test_final_2020_with_ae_scores.csv` | 555,719 | 38 | 2020-06-21 → 2020-12-31 | 0.386% (2,145 frauds) | Kafka producer (stream replay), evaluation |

Both files are comma-separated, have a header row, and contain the label column `is_fraud` (1 = fraud).

## Download

1. Download the archive from the Zenodo record above (`data.zip`).
2. Extract it so that the two CSV files sit directly in this folder (`data/`):

```bash
# from the repository root
unzip data.zip -d data/
```

Expected layout:

```
data/
├── README.md
├── train_2019_with_ae_scores.csv
└── test_final_2020_with_ae_scores.csv
```

The code reads these paths by default (see `src/config/settings.py`). To keep the files elsewhere, set the `DATA_DIR` environment variable, or override individual paths with `DICE_REFERENCE_DATA_PATH` and `PRODUCER_DATA_PATH`.

## Integrity (SHA-256)

```
8c8463c07e3440ea247b9efe96ebda3668e4fc1fd58bd1c1f853176921b3892d  train_2019_with_ae_scores.csv
a2d892e7582ab08ea3272717e2487cbf2a33788211da45b697b19a5a81525cf9  test_final_2020_with_ae_scores.csv
```

Verify after extracting:

```bash
cd data && sha256sum -c <<'EOF'
8c8463c07e3440ea247b9efe96ebda3668e4fc1fd58bd1c1f853176921b3892d  train_2019_with_ae_scores.csv
a2d892e7582ab08ea3272717e2487cbf2a33788211da45b697b19a5a81525cf9  test_final_2020_with_ae_scores.csv
EOF
```

## Source and derivation

The transactions come from the synthetic Sparkov Credit Card Transactions dataset ([Kaggle: kartik2112/fraud-detection](https://www.kaggle.com/datasets/kartik2112/fraud-detection)). All records are synthetic and do not describe real cardholders or transactions. Please refer to the original dataset page for its license and terms.

The original raw columns are kept, and the following were derived:

- **Temporal:** `hour`, `day_of_week`, `month`, `is_weekend`, `is_night`
- **Demographic / geospatial:** `age`, `geo_distance_km`, `geo_distance_log`
- **Channel flags:** `is_online`, `is_pos`
- **Behavioral / velocity:** `prev_trans_time`, `prev_amt`, `time_since_last_tx`, `tx_count_1h`, `amt_sum_24h`, `amt_mean_hist`, `amt_vs_mean`
- **`ae_anomaly_score`:** reconstruction error of an Autoencoder trained only on normal (non-fraud) transactions (see `src/ml/train_ae.ipynb`). It is used as an additional input feature for the LightGBM stage (see `src/ml/train_lightgbm.ipynb`).

The split is time-based (train: 2019, test: 2020), which also makes it suitable for concept-drift analysis.

## Citation

If you use this data, please cite the Zenodo record (DOI above), the accompanying paper, and the original Sparkov dataset.

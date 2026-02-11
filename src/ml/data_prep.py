import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler

AE_FEATURES = [
    "amount_ngn",
    "spending_deviation_score",
    "velocity_score",
    "user_avg_txn_amt",
    "user_std_txn_amt",
    "txn_hour",
    "is_night_txn",
    "user_txn_frequency_24h",
    "txn_count_last_1h",
    "avg_gap_between_txns",
    "device_seen_count",
    "is_device_shared",
    "new_device_transaction",
    "geospatial_velocity_anomaly"
]

def load_dataset(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    return df


def validate_columns(df: pd.DataFrame, required_cols: list):
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
# autoencoder feature matrix
def build_ae_feature_matrix(df: pd.DataFrame) -> pd.DataFrame:
    validate_columns(df, AE_FEATURES)
    X = df[AE_FEATURES].copy()
    return X

# autoencoder sadece normal ile eğitilecek, fraud evaluation için kullanılacak
def split_normal_fraud(df: pd.DataFrame):
    normal_df = df[df["is_fraud"] == 0]
    fraud_df = df[df["is_fraud"] == 1]
    return normal_df, fraud_df

def scale_features(X_normal: pd.DataFrame, X_fraud: pd.DataFrame):
    scaler = StandardScaler()

    # Fit ONLY on normal data
    X_normal_scaled = scaler.fit_transform(X_normal)

    # Transform fraud data
    X_fraud_scaled = scaler.transform(X_fraud)

    return X_normal_scaled, X_fraud_scaled, scaler

def prepare_ae_data(csv_path: str):
    df = load_dataset(csv_path)

    normal_df, fraud_df = split_normal_fraud(df)

    X_normal = build_ae_feature_matrix(normal_df)
    X_fraud = build_ae_feature_matrix(fraud_df)

    X_normal_scaled, X_fraud_scaled, scaler = scale_features(X_normal, X_fraud)

    return X_normal_scaled, X_fraud_scaled, scaler




if __name__ == "__main__":
    X_normal, X_fraud, scaler = prepare_ae_data("C:\\Users\\mustafa\\Desktop\\bilgisayara aktarılacaklar\\dersler\\ege yüksek dersler\\tez\\datasetler\\huggingface\\3-nigerian\\financial-fraud-dataset-train.csv")

    print("Normal scaled shape:", X_normal.shape)
    print("Fraud scaled shape:", X_fraud.shape)

    print("Mean (first feature):", X_normal[:, 0].mean())
    print("Std (first feature):", X_normal[:, 0].std())

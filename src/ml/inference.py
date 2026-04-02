"""
ML Inference Engine — Hibrit Pipeline (AE → LightGBM → SHAP)
=============================================================
Kafka consumer tarafından her işlem için çağrılır.
Tüm model yükleme ve tahmin mantığı burada merkezileştirilmiştir.

Pipeline sırası:
  1. Autoencoder → ae_anomaly_score üret
  2. LightGBM   → fraud probability üret
  3. TreeSHAP    → top-N feature katkıları üret

Kritik pattern: Her zaman LGBMWrapper kullan, raw Booster değil.
"""

import json
import time
import logging
import numpy as np
import pandas as pd
import joblib
import shap
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="shap")

from config.settings import (
    LGBM_MODEL_PATH, LGBM_CONTRACT_PATH,
    AE_MODEL_PATH, AE_SCALER_PATH,
    LGBM_FEATURES, CATEGORICAL_FEATURES,
)

logger = logging.getLogger(__name__)


# =============================================================================
# LightGBM Wrapper (categorical_feature uyumluluğu için)
# =============================================================================
class LGBMWrapper:
    """
    Raw LightGBM Booster'ı predict_proba API'si ile sarar.
    category/gender string → int dönüşümünü _prepare() içinde yapar.
    DiCE, drift, inference — her yerde BU wrapper kullanılmalı.
    """

    def __init__(self, booster, feature_names, categorical_features):
        self.booster = booster
        self.feature_names = feature_names
        self.categorical_features = categorical_features

    def _prepare(self, X):
        if not isinstance(X, pd.DataFrame):
            X = pd.DataFrame(X, columns=self.feature_names)
        X = X[self.feature_names].copy()
        for col in self.categorical_features:
            if col in X.columns and X[col].dtype == object:
                X[col] = X[col].astype("category").cat.codes
        return X

    def predict(self, X):
        X = self._prepare(X)
        return self.booster.predict(X.values)

    def predict_proba(self, X):
        preds = self.predict(X)
        return np.column_stack([1 - preds, preds])


# =============================================================================
# Inference Engine
# =============================================================================
class FraudInferenceEngine:
    """
    Tüm modelleri yükler ve tek bir predict() çağrısı ile
    AE → LightGBM → SHAP pipeline'ını çalıştırır.
    """

    def __init__(self):
        self.lgbm_model = None
        self.ae_model = None
        self.ae_scaler = None
        self.shap_explainer = None
        self.optimal_threshold = 0.5
        self.feature_names = LGBM_FEATURES
        self._loaded = False

    def load_models(self):
        """Tüm modelleri diskten yükle."""
        logger.info("Modeller yükleniyor...")

        # --- LightGBM ---
        raw_booster = joblib.load(LGBM_MODEL_PATH)
        self.lgbm_model = LGBMWrapper(
            raw_booster, LGBM_FEATURES, CATEGORICAL_FEATURES
        )
        logger.info(f"  ✓ LightGBM yüklendi: {LGBM_MODEL_PATH}")

        # --- Feature Contract (threshold) ---
        with open(LGBM_CONTRACT_PATH, "r") as f:
            contract = json.load(f)
        self.optimal_threshold = contract.get("optimal_threshold", 0.5)
        logger.info(f"  ✓ Optimal threshold: {self.optimal_threshold}")

        # --- Autoencoder (opsiyonel — CSV'de ae_anomaly_score varsa atlanabilir) ---
        try:
            import tensorflow as tf
            self.ae_model = tf.keras.models.load_model(AE_MODEL_PATH)
            self.ae_scaler = joblib.load(AE_SCALER_PATH)
            logger.info(f"  ✓ Autoencoder yüklendi: {AE_MODEL_PATH}")
        except Exception as e:
            logger.warning(f"  ⚠ AE yüklenemedi (CSV'deki skor kullanılacak): {e}")
            self.ae_model = None

        # --- SHAP Explainer ---
        self.shap_explainer = shap.TreeExplainer(
            raw_booster,
            feature_perturbation="tree_path_dependent",
        )
        logger.info("  ✓ SHAP TreeExplainer oluşturuldu")

        self._loaded = True
        logger.info("Tüm modeller başarıyla yüklendi.")

    def predict(self, transaction: dict) -> dict:
        """
        Tek bir işlem için tam pipeline çalıştır.

        Args:
            transaction: Feature-engineered işlem dict'i
                         (Sparkov CSV'den gelen tüm feature'lar dahil)

        Returns:
            dict: {
                "fraud_probability": float,
                "is_fraud": bool,
                "ae_anomaly_score": float,
                "risk_level": str,        # HIGH / MEDIUM / LOW
                "shap_explanation": [...], # Top-5 feature katkısı
                "latency_ms": float,
            }
        """
        if not self._loaded:
            raise RuntimeError("Modeller henüz yüklenmedi. load_models() çağırın.")

        t_start = time.perf_counter()

        # --- 1. Feature DataFrame oluştur ---
        df = pd.DataFrame([transaction])[LGBM_FEATURES]

        # --- 2. AE anomaly score (CSV'de varsa kullan, yoksa hesapla) ---
        ae_score = transaction.get("ae_anomaly_score")
        if ae_score is None and self.ae_model is not None:
            ae_score = self._compute_ae_score(transaction)
            df["ae_anomaly_score"] = ae_score

        # --- 3. LightGBM tahmin ---
        fraud_prob = float(self.lgbm_model.predict(df)[0])
        is_fraud = fraud_prob >= self.optimal_threshold

        # --- 4. Risk seviyesi ---
        if fraud_prob >= 0.9:
            risk_level = "HIGH"
        elif fraud_prob >= 0.5:
            risk_level = "MEDIUM"
        else:
            risk_level = "LOW"

        # --- 5. SHAP açıklama (senkron, ~5ms) ---
        shap_explanation = self._explain(df)

        latency_ms = (time.perf_counter() - t_start) * 1000

        return {
            "fraud_probability": round(fraud_prob, 6),
            "is_fraud": is_fraud,
            "ae_anomaly_score": round(float(ae_score or 0), 4),
            "risk_level": risk_level,
            "threshold": self.optimal_threshold,
            "shap_explanation": shap_explanation,
            "latency_ms": round(latency_ms, 2),
        }

    def _compute_ae_score(self, transaction: dict) -> float:
        """Autoencoder ile anomaly score hesapla (AE model yüklüyse)."""
        from config.settings import AE_FEATURES
        features = [transaction.get(f, 0) for f in AE_FEATURES]
        X = np.array([features])
        X_scaled = self.ae_scaler.transform(X)
        X_reconstructed = self.ae_model.predict(X_scaled, verbose=0)
        mse = float(np.mean((X_scaled - X_reconstructed) ** 2))
        return mse

    def _explain(self, df: pd.DataFrame, top_n: int = 5) -> list:
        """
        TreeSHAP ile top-N feature katkısını döndür.
        Kafka-ready JSON formatında.
        """
        try:
            df_prepared = self.lgbm_model._prepare(df)
            shap_values = self.shap_explainer.shap_values(df_prepared.values)

            # Binary classification: shap_values[1] = fraud class
            if isinstance(shap_values, list):
                sv = shap_values[1][0]
            else:
                sv = shap_values[0]

            # Top-N (mutlak değere göre)
            indices = np.argsort(np.abs(sv))[::-1][:top_n]

            explanation = []
            for idx in indices:
                feat_name = self.feature_names[idx]
                feat_val = df.iloc[0][feat_name]
                contribution = float(sv[idx])
                explanation.append({
                    "feature": feat_name,
                    "value": feat_val if not isinstance(feat_val, (np.floating, np.integer)) else round(float(feat_val), 4),
                    "shap_contribution": round(contribution, 4),
                    "direction": "fraud" if contribution > 0 else "normal",
                })

            return explanation

        except Exception as e:
            logger.error(f"SHAP açıklama hatası: {e}")
            return []
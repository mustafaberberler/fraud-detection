"""
ML Inference Engine — Hybrid Pipeline (AE → LightGBM → SHAP)
=============================================================
Called by the Kafka consumer for every transaction.
All model loading and prediction logic is centralized here.

Pipeline order:
  1. Autoencoder → produce ae_anomaly_score
  2. LightGBM   → produce fraud probability
  3. TreeSHAP    → produce top-N feature contributions

Critical pattern: Always use LGBMWrapper, not the raw Booster.
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
    DICE_REFERENCE_DATA_PATH,
)

logger = logging.getLogger(__name__)


# =============================================================================
# LightGBM Wrapper (for categorical_feature compatibility)
# =============================================================================
class LGBMWrapper:
    """
    Wraps the raw LightGBM Booster with a predict_proba API.
    Performs the category/gender string → int conversion inside _prepare().
    DiCE, drift, inference — THIS wrapper must be used everywhere.
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
    Loads all the models and runs the AE → LightGBM → SHAP pipeline
    with a single predict() call.
    """

    def __init__(self):
        self.lgbm_model = None
        self.ae_model = None
        self.ae_scaler = None
        self.shap_explainer = None
        self.optimal_threshold = 0.5
        self.feature_names = LGBM_FEATURES
        self._dice_reference_df = None
        self._loaded = False

    def load_models(self):
        """Load all the models from disk."""
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
        self._feature_ranges = contract.get("feature_ranges", {})  # ← added
        logger.info(f"  ✓ Optimal threshold: {self.optimal_threshold}")

        # --- Autoencoder (optional — can be skipped if ae_anomaly_score is in the CSV) ---
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

        # --- Reference data for DiCE ---
        try:
            df_ref_raw = pd.read_csv(DICE_REFERENCE_DATA_PATH, nrows=500)[LGBM_FEATURES].copy()
            for col in CATEGORICAL_FEATURES:
                if col in df_ref_raw.columns:
                    df_ref_raw[col] = df_ref_raw[col].astype("category").cat.codes

            # Generate the is_fraud label from the model
            preds = self.lgbm_model.predict(df_ref_raw)
            df_ref_raw["is_fraud"] = (preds >= self.optimal_threshold).astype(int)

            self._dice_reference_df = df_ref_raw
            logger.info(f"  ✓ DiCE referans verisi yüklendi (500 satır, "
                f"{df_ref_raw['is_fraud'].sum()} fraud)")
        except Exception as e:
            logger.warning(
                f"  ⚠ DiCE referans verisi yüklenemedi ({DICE_REFERENCE_DATA_PATH}): {e}"
            )
            self._dice_reference_df = None

        self._loaded = True
        logger.info("Tüm modeller başarıyla yüklendi.")

    def predict(self, transaction: dict) -> dict:
        """
        Run the full pipeline for a single transaction.

        Args:
            transaction: Feature-engineered transaction dict
                         (including all the features from the Sparkov CSV)

        Returns:
            dict: {
                "fraud_probability": float,
                "is_fraud": bool,
                "ae_anomaly_score": float,
                "risk_level": str,        # HIGH / MEDIUM / LOW
                "shap_explanation": [...], # Top-5 feature contributions
                "latency_ms": float,
            }
        """
        if not self._loaded:
            raise RuntimeError("Modeller henüz yüklenmedi. load_models() çağırın.")

        t_start = time.perf_counter()

        # --- 1. Create the feature DataFrame ---
        df = pd.DataFrame([transaction])[LGBM_FEATURES]

        # --- 2. AE anomaly score (use it if it is in the CSV, otherwise compute it) ---
        ae_score = transaction.get("ae_anomaly_score")
        if ae_score is None and self.ae_model is not None:
            ae_score = self._compute_ae_score(transaction)
            df["ae_anomaly_score"] = ae_score

        # --- 3. LightGBM prediction ---
        fraud_prob = float(self.lgbm_model.predict(df)[0])
        is_fraud = fraud_prob >= self.optimal_threshold

        # --- 4. Risk level ---
        if fraud_prob >= 0.9:
            risk_level = "HIGH"
        elif fraud_prob >= 0.5:
            risk_level = "MEDIUM"
        else:
            risk_level = "LOW"

        # --- 5. SHAP explanation (synchronous, ~5ms) ---
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
        """Compute the anomaly score with the Autoencoder (if the AE model is loaded)."""
        from config.settings import AE_FEATURES
        features = [transaction.get(f, 0) for f in AE_FEATURES]
        X = np.array([features])
        X_scaled = self.ae_scaler.transform(X)
        X_reconstructed = self.ae_model.predict(X_scaled, verbose=0)
        mse = float(np.mean((X_scaled - X_reconstructed) ** 2))
        return mse

    def _explain(self, df: pd.DataFrame, top_n: int = 5) -> list:
        """
        Return the top-N feature contributions with TreeSHAP.
        In Kafka-ready JSON format.
        """
        try:
            df_prepared = self.lgbm_model._prepare(df)
            shap_values = self.shap_explainer.shap_values(df_prepared.values)

            # Binary classification: shap_values[1] = fraud class
            if isinstance(shap_values, list):
                sv = shap_values[1][0]
            else:
                sv = shap_values[0]

            # Top-N (by absolute value)
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
        
    def explain_counterfactual(self, transaction: dict, prediction: dict) -> dict | None:
        """
        Generate a counterfactual explanation with DiCE.
        Called only for fraud transactions, triggered from an async thread.
        """
        try:
            import dice_ml

            if self._dice_reference_df is None:
                logger.warning("DiCE referans verisi yok, atlanıyor.")
                return None

            # --- Feature DataFrame ---
            df = pd.DataFrame([transaction])[LGBM_FEATURES].copy()

            # Categorical encoding (same logic as LGBMWrapper._prepare)
            df_encoded = self.lgbm_model._prepare(df)

            # DiCE requires the is_fraud column (outcome column)
            #df_encoded["is_fraud"] = int(prediction["is_fraud"])



            # --- Continuous / categorical split ---
            continuous = [
                f for f in LGBM_FEATURES
                if f not in CATEGORICAL_FEATURES
            ]
            # Categoricals are encoded (int), DiCE sees them as continuous
            # This is the most stable approach (compatible with backend="sklearn")

            d = dice_ml.Data(
                #dataframe=df_encoded,
                dataframe=self._dice_reference_df,  # ← added: reference data
                continuous_features=continuous,
                outcome_name="is_fraud",
            )
            m = dice_ml.Model(
                model=self.lgbm_model,
                backend="sklearn",
                model_type="classifier",
            )
            exp = dice_ml.Dice(d, m, method="random")

            cf_result = exp.generate_counterfactuals(
                df_encoded,
                total_CFs=1,
                desired_class="opposite",
                verbose=False,
            )

            cf_df = cf_result.cf_examples_list[0].final_cfs_df
            if cf_df is None or cf_df.empty:
                return None

            # --- Original vs CF difference dict ---
            cf_dict = {}
            for feat in LGBM_FEATURES:
                orig_val = float(df_encoded[feat].iloc[0])
                cf_val   = float(cf_df[feat].iloc[0])
                if abs(orig_val - cf_val) > 1e-6:
                    # Unit guess
                    if "score" in feat or feat == "ae_anomaly_score":
                        unit = "score"
                    elif feat == "amt" or "amt" in feat:
                        unit = "$"
                    else:
                        unit = "value"

                    cf_dict[feat] = {
                        "original": round(orig_val, 4),
                        "cf":       round(cf_val, 4),
                        "unit":     unit,
                    }

            return cf_dict if cf_dict else None

        except Exception as e:
            logger.error(f"explain_counterfactual hatası: {e}")
            return None
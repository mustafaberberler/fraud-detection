"""
Gerçek Zamanlı Drift Monitor — PSI / KL Divergence
===================================================
Kafka consumer pipeline'ına entegre edilir.
Her işlem accumulate edilir, window dolunca drift kontrolü yapılır.
Drift tespit edilirse drift-alerts topic'ine alarm gönderilir.

Tez önerisindeki taahhüt:
  "PSI ve KL Divergence ile veri dağılımı izleme,
   drift tespit edildiğinde eşikler adaptif güncellenecek."
"""

import json
import time
import logging
import numpy as np
from datetime import datetime
from collections import deque

from config.settings import (
    REFERENCE_DIST_PATH,
    LGBM_FEATURES, CATEGORICAL_FEATURES,
    PSI_THRESHOLDS, KL_THRESHOLDS,
    DRIFT_WINDOW_SIZE, DRIFT_CHECK_INTERVAL,
)

logger = logging.getLogger(__name__)


class DriftMonitor:
    """
    Sliding-window tabanlı gerçek zamanlı drift izleyici.

    Kullanım:
        monitor = DriftMonitor()
        monitor.load_reference()

        for transaction in kafka_stream:
            monitor.accumulate(transaction)
            if monitor.should_check():
                alert = monitor.check_drift()
                if alert:
                    kafka_producer.send("drift-alerts", alert)
    """

    def __init__(self, window_size=DRIFT_WINDOW_SIZE,
                 check_interval=DRIFT_CHECK_INTERVAL):
        self.window_size = window_size
        self.check_interval = check_interval

        self.reference_stats = {}
        self.buffer = {feat: deque(maxlen=window_size) for feat in LGBM_FEATURES}
        self.categorical_buffer = {feat: deque(maxlen=window_size) for feat in CATEGORICAL_FEATURES}

        self.transaction_count = 0
        self.last_check_count = 0
        self.last_drift_result = None
        self._loaded = False

    def load_reference(self):
        """Referans dağılım istatistiklerini yükle (training verisinden)."""
        try:
            with open(REFERENCE_DIST_PATH, "r") as f:
                self.reference_stats = json.load(f)
            logger.info(f"  ✓ Referans dağılımlar yüklendi ({len(self.reference_stats)} feature)")
            self._loaded = True
        except FileNotFoundError:
            logger.error(f"  ✗ Referans dosyası bulunamadı: {REFERENCE_DIST_PATH}")
            self._loaded = False

    def accumulate(self, transaction: dict):
        """Yeni işlemi sliding window'a ekle."""
        for feat in LGBM_FEATURES:
            val = transaction.get(feat)
            if val is not None:
                if feat in CATEGORICAL_FEATURES:
                    self.categorical_buffer[feat].append(val)
                else:
                    try:
                        self.buffer[feat].append(float(val))
                    except (ValueError, TypeError):
                        pass
        self.transaction_count += 1

    def should_check(self) -> bool:
        """Drift kontrolü zamanı geldi mi?"""
        return (
            self._loaded
            and self.transaction_count - self.last_check_count >= self.check_interval
            and len(self.buffer[LGBM_FEATURES[0]]) >= self.window_size // 2
        )

    def check_drift(self) -> dict | None:
        """
        Tüm feature'lar için PSI ve KL hesapla.
        Drift varsa Kafka-ready alert JSON döndür, yoksa None.
        """
        self.last_check_count = self.transaction_count
        t_start = time.perf_counter()

        results = {}
        for feat in LGBM_FEATURES:
            if feat in CATEGORICAL_FEATURES:
                psi = self._psi_categorical(feat)
                kl = psi
            else:
                psi = self._psi_numeric(feat)
                kl = self._kl_numeric(feat)

            psi_status = self._classify(psi, PSI_THRESHOLDS)
            kl_status = self._classify(kl, KL_THRESHOLDS)

            results[feat] = {
                "psi": round(psi, 6),
                "kl_divergence": round(kl, 6),
                "psi_status": psi_status,
                "kl_status": kl_status,
            }

        # Kritik feature'ları bul
        critical = [
            {"feature": f, "psi": r["psi"], "kl_divergence": r["kl_divergence"]}
            for f, r in results.items()
            if r["psi_status"] == "CRITICAL"
        ]
        warning = [f for f, r in results.items() if r["psi_status"] == "WARNING"]

        avg_psi = np.mean([r["psi"] for r in results.values()])
        max_psi = max(r["psi"] for r in results.values())
        max_feat = max(results, key=lambda f: results[f]["psi"])

        retrain_required = len(critical) >= 2 or max_psi >= 0.5

        latency_ms = (time.perf_counter() - t_start) * 1000

        self.last_drift_result = results

        # Sadece warning veya critical varsa alert üret
        if len(critical) > 0 or len(warning) > 0:
            alert = {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "alert_type": "drift_monitoring",
                "overall_status": "CRITICAL" if retrain_required else
                                  "WARNING" if critical else "STABLE",
                "metrics": {
                    "avg_psi": round(float(avg_psi), 6),
                    "max_psi": round(float(max_psi), 6),
                    "max_psi_feature": max_feat,
                    "n_critical_features": len(critical),
                    "n_warning_features": len(warning),
                    "n_total_features": len(LGBM_FEATURES),
                    "window_size": len(self.buffer[LGBM_FEATURES[0]]),
                    "total_transactions": self.transaction_count,
                },
                "critical_features": critical,
                "actions": {
                    "retrain_required": retrain_required,
                    "threshold_recalibration": retrain_required,
                },
                "check_latency_ms": round(latency_ms, 2),
                "model_version": "lgbm_hybrid_v1",
            }
            logger.warning(
                f"DRIFT ALERT: {alert['overall_status']} — "
                f"{len(critical)} critical, {len(warning)} warning features "
                f"(max PSI={max_psi:.4f} @ {max_feat})"
            )
            return alert

        logger.info(
            f"Drift check OK: tüm feature'lar stabil "
            f"(max PSI={max_psi:.4f} @ {max_feat}, "
            f"window={len(self.buffer[LGBM_FEATURES[0]])})"
        )
        return None

    # -------------------------------------------------------------------------
    # PSI / KL hesaplama (drift_monitoring.ipynb ile aynı formüller)
    # -------------------------------------------------------------------------
    def _psi_numeric(self, feature: str, n_bins: int = 10) -> float:
        """Numeric feature için PSI hesapla."""
        current = np.array(self.buffer[feature])
        if len(current) < 50:
            return 0.0

        ref = self.reference_stats.get(feature, {})
        if not ref or ref.get("type") != "numeric":
            return 0.0

        eps = 1e-6
        ref_min, ref_max = ref["min"], ref["max"]
        breakpoints = np.linspace(
            min(ref_min, current.min()),
            max(ref_max, current.max()),
            n_bins + 1,
        )

        # Referans dağılımı istatistiklerden yeniden oluştur (histogram yok,
        # mean/std'den normal dağılım varsayımı ile bin oranları hesapla)
        from scipy.stats import norm
        ref_cdf = norm.cdf(breakpoints, loc=ref["mean"], scale=ref["std"])
        ref_pct = np.diff(ref_cdf)
        ref_pct = np.clip(ref_pct, eps, None)
        ref_pct = ref_pct / ref_pct.sum()

        cur_counts = np.histogram(current, bins=breakpoints)[0]
        cur_pct = (cur_counts + eps) / (cur_counts.sum() + eps * n_bins)

        psi = float(np.sum((cur_pct - ref_pct) * np.log(cur_pct / ref_pct)))
        return max(psi, 0.0)

    def _kl_numeric(self, feature: str, n_bins: int = 10) -> float:
        """Simetrik KL Divergence."""
        current = np.array(self.buffer[feature])
        if len(current) < 50:
            return 0.0

        ref = self.reference_stats.get(feature, {})
        if not ref or ref.get("type") != "numeric":
            return 0.0

        eps = 1e-6
        ref_min, ref_max = ref["min"], ref["max"]
        breakpoints = np.linspace(
            min(ref_min, current.min()),
            max(ref_max, current.max()),
            n_bins + 1,
        )

        from scipy.stats import norm, entropy
        ref_cdf = norm.cdf(breakpoints, loc=ref["mean"], scale=ref["std"])
        ref_pct = np.diff(ref_cdf)
        ref_pct = np.clip(ref_pct, eps, None)
        ref_pct = ref_pct / ref_pct.sum()

        cur_counts = np.histogram(current, bins=breakpoints)[0]
        cur_pct = (cur_counts + eps) / (cur_counts.sum() + eps * n_bins)

        kl_fwd = float(entropy(ref_pct, cur_pct))
        kl_rev = float(entropy(cur_pct, ref_pct))
        return (kl_fwd + kl_rev) / 2

    def _psi_categorical(self, feature: str) -> float:
        """Categorical feature için PSI."""
        current = list(self.categorical_buffer[feature])
        if len(current) < 50:
            return 0.0

        ref = self.reference_stats.get(feature, {})
        if not ref or ref.get("type") != "categorical":
            return 0.0

        eps = 1e-6
        ref_counts = ref.get("value_counts", {})

        # Current dağılım
        from collections import Counter
        cur_counter = Counter(current)
        total = sum(cur_counter.values())
        cur_counts = {k: v / total for k, v in cur_counter.items()}

        all_cats = set(ref_counts.keys()) | set(cur_counts.keys())
        psi = 0.0
        for cat in all_cats:
            ref_pct = ref_counts.get(cat, eps)
            cur_pct = cur_counts.get(cat, eps)
            psi += (cur_pct - ref_pct) * np.log(cur_pct / ref_pct)

        return max(float(psi), 0.0)

    @staticmethod
    def _classify(value: float, thresholds: dict) -> str:
        if value >= thresholds["critical"]:
            return "CRITICAL"
        elif value >= thresholds["warning"]:
            return "WARNING"
        return "STABLE"
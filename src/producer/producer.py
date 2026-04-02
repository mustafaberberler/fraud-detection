"""
Kafka Producer — Sparkov Transaction Streamer
==============================================
Feature-engineered CSV dosyasından gerçek kayıtları okuyup
Kafka'ya stream eder. Kontrol edilebilir hızda (TPS) çalışır.

Kullanım:
    python -m producer.producer
    python -m producer.producer --tps 500 --data ./data/test.csv

Mevcut (dummy) producer'dan farkları:
  - Gerçek Sparkov verisi kullanır (rastgele değil)
  - Feature-engineered 20 feature + ham alanlar
  - Kontrol edilebilir TPS (throughput testi için)
  - Kafka timestamp + partition key desteği
"""

import json
import time
import argparse
import logging
import signal
import sys

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPICS,
    PRODUCER_DATA_PATH,
    PRODUCER_RATE_TPS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Graceful shutdown
_running = True


def signal_handler(sig, frame):
    global _running
    logger.info("Shutdown sinyali alındı, durduruluyor...")
    _running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def create_producer() -> KafkaProducer:
    """Kafka producer oluştur (JSON serializer ile)."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        linger_ms=5,       # Küçük batch'ler için düşük latency
        batch_size=16384,
    )


def load_data(path: str) -> pd.DataFrame:
    """CSV dosyasını yükle."""
    logger.info(f"Veri yükleniyor: {path}")
    df = pd.read_csv(path)
    logger.info(f"  {len(df):,} kayıt yüklendi, {df.shape[1]} kolon")

    # is_fraud varsa dağılımı göster
    if "is_fraud" in df.columns:
        fraud_count = df["is_fraud"].sum()
        logger.info(f"  Fraud: {fraud_count:,} ({fraud_count/len(df)*100:.2f}%)")

    return df


def stream_transactions(producer: KafkaProducer, df: pd.DataFrame,
                        tps: int, topic: str):
    """
    DataFrame'deki kayıtları Kafka'ya belirli TPS hızında stream et.

    Args:
        producer: Kafka producer
        df: Feature-engineered DataFrame
        tps: Hedef transaction per second
        topic: Kafka topic adı
    """
    global _running

    interval = 1.0 / tps if tps > 0 else 0
    sent_count = 0
    error_count = 0
    start_time = time.time()

    logger.info(f"Streaming başlıyor → topic: {topic}")
    logger.info(f"  Hedef TPS: {tps}")
    logger.info(f"  Toplam kayıt: {len(df):,}")

    for idx, row in df.iterrows():
        if not _running:
            break

        # DataFrame satırını JSON'a çevir
        record = row.to_dict()

        # NaN temizliği
        record = {
            k: (None if pd.isna(v) else v)
            for k, v in record.items()
        }

        # Partition key = customer ID (aynı müşterinin işlemleri aynı partition'a)
        partition_key = str(record.get("cc_num", ""))

        try:
            producer.send(
                topic,
                key=partition_key,
                value=record,
            )
            sent_count += 1

            # İlerleme raporu (her 1000 işlemde)
            if sent_count % 1000 == 0:
                elapsed = time.time() - start_time
                actual_tps = sent_count / elapsed if elapsed > 0 else 0
                logger.info(
                    f"  Gönderildi: {sent_count:>8,} / {len(df):,}  "
                    f"| TPS: {actual_tps:,.0f}  "
                    f"| Süre: {elapsed:.1f}s"
                )

        except KafkaError as e:
            error_count += 1
            if error_count <= 5:
                logger.error(f"Kafka gönderim hatası: {e}")

        # Rate limiting
        if interval > 0:
            time.sleep(interval)

    # Flush
    producer.flush()

    elapsed = time.time() - start_time
    actual_tps = sent_count / elapsed if elapsed > 0 else 0

    logger.info(f"\nStreaming tamamlandı:")
    logger.info(f"  Gönderilen: {sent_count:,}")
    logger.info(f"  Hatalı:     {error_count:,}")
    logger.info(f"  Süre:       {elapsed:.1f}s")
    logger.info(f"  Gerçek TPS: {actual_tps:,.0f}")


def main():
    parser = argparse.ArgumentParser(description="Sparkov Transaction Producer")
    parser.add_argument("--data", default=PRODUCER_DATA_PATH, help="CSV dosya yolu")
    parser.add_argument("--tps", type=int, default=PRODUCER_RATE_TPS, help="Hedef TPS")
    parser.add_argument("--limit", type=int, default=None, help="Gönderi limiti")
    parser.add_argument("--loop", action="store_true", help="Veri bitince başa sar")
    args = parser.parse_args()

    # Veri yükle
    df = load_data(args.data)
    if args.limit:
        df = df.head(args.limit)
        logger.info(f"  Limit uygulandı: {args.limit:,} kayıt")

    # Producer oluştur
    producer = create_producer()
    logger.info(f"  Kafka producer hazır: {KAFKA_BOOTSTRAP_SERVERS}")

    topic = KAFKA_TOPICS["raw_transactions"]

    try:
        if args.loop:
            cycle = 0
            while _running:
                cycle += 1
                logger.info(f"--- Döngü #{cycle} ---")
                stream_transactions(producer, df, args.tps, topic)
        else:
            stream_transactions(producer, df, args.tps, topic)
    finally:
        producer.close()
        logger.info("Producer kapatıldı.")


if __name__ == "__main__":
    main()
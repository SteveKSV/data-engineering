import csv
import json
import os
import time
import logging
from kafka import KafkaProducer, KafkaAdminClient
from kafka.errors import KafkaError, NoBrokersAvailable, UnknownTopicOrPartitionError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
CSV_FILE_PATH     = os.getenv("CSV_FILE_PATH", "data/Divvy_Trips_2019_Q4.csv")
TOPIC1            = os.getenv("TOPIC1", "Topic1")
TOPIC2            = os.getenv("TOPIC2", "Topic2")
BATCH_SIZE        = int(os.getenv("BATCH_SIZE", "500"))
DELAY_SECONDS     = float(os.getenv("DELAY_SECONDS", "0.001"))


def wait_for_topics(topics: list, retries: int = 20, delay: int = 5):
    """Block until all required topics exist in the cluster."""
    for attempt in range(1, retries + 1):
        try:
            admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS, request_timeout_ms=10000)
            existing = set(admin.list_topics())
            admin.close()
            missing = [t for t in topics if t not in existing]
            if not missing:
                logger.info("Topics confirmed: %s", topics)
                return
            logger.warning("Attempt %d/%d: topics not ready yet: %s — retrying in %ds…",
                           attempt, retries, missing, delay)
        except Exception as exc:
            logger.warning("Attempt %d/%d: cannot reach broker (%s) — retrying in %ds…",
                           attempt, retries, exc, delay)
        time.sleep(delay)

    raise RuntimeError("Topics %s did not appear after %d attempts" % (topics, retries))


def create_producer(retries: int = 10, delay: int = 5) -> KafkaProducer:
    """Create Kafka producer with retry logic."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                linger_ms=10,
                compression_type="gzip",
            )
            logger.info("Connected to Kafka brokers: %s", BOOTSTRAP_SERVERS)
            return producer
        except NoBrokersAvailable:
            logger.warning("Attempt %d/%d: brokers not available, retrying in %ds…",
                           attempt, retries, delay)
            time.sleep(delay)

    raise RuntimeError("Could not connect to Kafka after %d attempts" % retries)


def delivery_report(record_metadata):
    logger.debug(
        "Delivered → topic=%s partition=%d offset=%d",
        record_metadata.topic, record_metadata.partition, record_metadata.offset,
    )


def error_report(exc):
    logger.error("Delivery failed: %s", exc)


def parse_row(row: dict) -> dict:
    return {
        "trip_id":           int(row["trip_id"]) if row["trip_id"] else None,
        "start_time":        row["start_time"],
        "end_time":          row["end_time"],
        "bikeid":            int(row["bikeid"]) if row["bikeid"] else None,
        "tripduration":      float(row["tripduration"].replace(",", "")) if row["tripduration"] else None,
        "from_station_id":   int(row["from_station_id"]) if row["from_station_id"] else None,
        "from_station_name": row["from_station_name"],
        "to_station_id":     int(row["to_station_id"]) if row["to_station_id"] else None,
        "to_station_name":   row["to_station_name"],
        "usertype":          row["usertype"],
        "gender":            row["gender"],
        "birthyear":         int(row["birthyear"]) if row["birthyear"] else None,
    }


def produce_messages():
    # Wait until both topics actually exist before touching the producer
    wait_for_topics([TOPIC1, TOPIC2])

    producer = create_producer()

    total_sent = 0
    errors = 0

    logger.info("Reading CSV: %s", CSV_FILE_PATH)
    logger.info("Publishing to topics: %s, %s", TOPIC1, TOPIC2)

    with open(CSV_FILE_PATH, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            try:
                message = parse_row(row)
                key = str(message["trip_id"])

                producer.send(TOPIC1, key=key, value=message).add_callback(delivery_report).add_errback(error_report)
                producer.send(TOPIC2, key=key, value=message).add_callback(delivery_report).add_errback(error_report)

                total_sent += 1

                if total_sent % BATCH_SIZE == 0:
                    producer.flush()
                    logger.info("Sent %d messages so far…", total_sent)
                    time.sleep(DELAY_SECONDS)

            except (ValueError, KeyError) as exc:
                errors += 1
                logger.warning("Skipping malformed row %s: %s", row.get("trip_id"), exc)

    producer.flush()
    logger.info("Done. Total messages sent: %d (errors skipped: %d)", total_sent, errors)
    producer.close()


if __name__ == "__main__":
    produce_messages()

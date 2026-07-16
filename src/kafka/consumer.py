import json
from typing import Callable, Any
from confluent_kafka import Consumer, KafkaError, KafkaException
from src.config import settings
from src.utils.logging import get_logger, log_record, log_record_error

logger = get_logger(__name__)

# BUG DF-01: auto.offset.reset='latest'
# When the consumer restarts (deploy, crash, scale-down), it resumes from the
# latest available offset — not from where it left off. Any messages produced
# during the downtime window are permanently skipped with no error or warning.
# In a 2-minute deploy window at 50,000 events/min, 100,000 records vanish silently.
# Fix: use auto.offset.reset='earliest' combined with committed offsets so the
# consumer always resumes from the last successfully committed position.

# BUG DF-02: No schema validation before processing.
# If a producer sends a malformed record (missing required fields, wrong types,
# truncated JSON), json.loads() or the handler function raises an exception that
# propagates out of the poll loop — crashing the entire consumer process.
# All subsequent messages in the partition are blocked until the consumer restarts.
# Fix: wrap each record in try/except, send invalid records to the DLQ, and
# continue processing. Never let one bad record stop the entire pipeline.


def get_consumer(group_id: str, topics: list[str]) -> Consumer:
    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",   # BUG DF-01: should be "earliest"
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
    })
    consumer.subscribe(topics)
    return consumer


def run_consumer(
    group_id: str,
    topics: list[str],
    handler: Callable[[dict], Any],
    dlq_topic: str | None = None,
) -> None:
    from src.kafka.dlq import send_to_dlq

    consumer = get_consumer(group_id, topics)
    logger.info("consumer_started", group_id=group_id, topics=topics)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            # BUG DF-02: no try/except here — any exception kills the consumer loop
            raw = json.loads(msg.value().decode("utf-8"))
            log_record(logger, raw)
            handler(raw)  # BUG DF-02: unhandled exception propagates here

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        logger.info("consumer_stopped", group_id=group_id)

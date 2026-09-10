import json
from typing import Any

from confluent_kafka import Consumer, Producer, KafkaException
from pydantic import ValidationError

from src.ingestion.schema import ProductEvent


import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

TOPIC = "product-events"
DLQ_TOPIC = "product-events-dlq"

GROUP_ID = "livecart-consumer"


def create_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )


def create_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        }
    )


def validate_event(raw_value: str) -> tuple[ProductEvent | None, str | None]:
    try:
        data = json.loads(raw_value)
        event = ProductEvent.model_validate(data)

        print(
            f"✅ Valid event: "
            f"event_id={event.event_id}, "
            f"product_id={event.product_id}"
        )

        return event, None

    except json.JSONDecodeError as exc:
        reason = f"JSONDecodeError: {exc}"
        print(f"❌ Rejected event: {reason}")
        return None, reason

    except ValidationError as exc:
        reason = f"ValidationError: {exc}"
        print(f"❌ Rejected event: {reason}")
        return None, reason


def send_to_dlq(
    producer: Producer,
    raw_value: str,
    reason: str,
    partition: int,
    offset: int,
) -> None:

    dlq_record: dict[str, Any] = {
        "raw_payload": raw_value,
        "rejection_reason": reason,
        "source_topic": TOPIC,
        "source_partition": partition,
        "source_offset": offset,
    }

    producer.produce(
        DLQ_TOPIC,
        value=json.dumps(dlq_record, ensure_ascii=False).encode("utf-8"),
    )

    producer.flush()

    print(
        f"🛑 Sent rejected event to DLQ: "
        f"topic={DLQ_TOPIC}, "
        f"partition={partition}, "
        f"offset={offset}"
    )


def consume_batch(max_messages: int = 10, timeout_seconds: int = 10) -> int:
    """
    Consume a finite batch for pipeline/orchestration execution.

    Valid events pass Pydantic validation.
    Invalid events are quarantined in Kafka DLQ with the raw payload
    and explicit rejection reason.
    """

    consumer = create_consumer()
    producer = create_producer()

    consumer.subscribe([TOPIC])

    processed = 0

    print(f"🚀 Listening to Kafka topic: {TOPIC}")
    print(f"🛑 DLQ topic: {DLQ_TOPIC}")

    try:
        while processed < max_messages:
            message = consumer.poll(1.0)

            if message is None:
                timeout_seconds -= 1

                if timeout_seconds <= 0:
                    break

                continue

            if message.error():
                raise KafkaException(message.error())

            raw_value = message.value().decode("utf-8")

            print(
                f"\n📩 Received event "
                f"partition={message.partition()} "
                f"offset={message.offset()}"
            )

            event, rejection_reason = validate_event(raw_value)

            if event is None:
                send_to_dlq(
                    producer=producer,
                    raw_value=raw_value,
                    reason=rejection_reason or "Unknown validation error",
                    partition=message.partition(),
                    offset=message.offset(),
                )

            # Invalid records are intentionally committed AFTER
            # successful DLQ publication so they don't get processed
            # repeatedly.
            consumer.commit(message=message, asynchronous=False)

            processed += 1

        print(
            f"\n✅ Ingestion batch complete: "
            f"{processed} message(s) processed"
        )

        return processed

    finally:
        producer.flush()
        consumer.close()


def consume_events() -> None:
    """
    Continuous consumer for live operation.
    """

    consumer = create_consumer()
    producer = create_producer()

    consumer.subscribe([TOPIC])

    print(f"🚀 Listening to Kafka topic: {TOPIC}")

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                raise KafkaException(message.error())

            raw_value = message.value().decode("utf-8")

            print(
                f"\n📩 Received event "
                f"partition={message.partition()} "
                f"offset={message.offset()}"
            )

            event, rejection_reason = validate_event(raw_value)

            if event is None:
                send_to_dlq(
                    producer=producer,
                    raw_value=raw_value,
                    reason=rejection_reason or "Unknown validation error",
                    partition=message.partition(),
                    offset=message.offset(),
                )

            consumer.commit(message=message, asynchronous=False)

    except KeyboardInterrupt:
        print("\n🛑 Consumer stopped.")

    finally:
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    consume_events()
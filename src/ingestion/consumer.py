import json

from confluent_kafka import Consumer, KafkaException
from pydantic import ValidationError

from src.ingestion.schema import ProductEvent


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "product-events"
GROUP_ID = "livecart-consumer"


def create_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )


def validate_event(raw_value: str) -> ProductEvent | None:
    try:
        data = json.loads(raw_value)
        event = ProductEvent.model_validate(data)

        print(
            f"✅ Valid event: "
            f"event_id={event.event_id}, "
            f"product_id={event.product_id}"
        )

        return event

    except (json.JSONDecodeError, ValidationError) as exc:
        print(f"❌ Rejected event: {exc}")
        return None


def consume_events() -> None:
    consumer = create_consumer()
    consumer.subscribe([TOPIC])

    print(f"🎧 Listening to Kafka topic: {TOPIC}")

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

            validate_event(raw_value)

    except KeyboardInterrupt:
        print("\n🛑 Consumer stopped.")

    finally:
        consumer.close()


if __name__ == "__main__":
    consume_events()
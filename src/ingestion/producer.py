import json

from confluent_kafka import Producer

from src.ingestion.schema import ProductEvent


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "product-events"


def create_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        }
    )


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"❌ Delivery failed: {err}")
        return

    print(
        f"✅ Delivered to {msg.topic()} "
        f"[partition={msg.partition()}] "
        f"offset={msg.offset()}"
    )


def publish_product_event(event: ProductEvent) -> None:
    producer = create_producer()

    payload = event.model_dump(mode="json")

    producer.produce(
        TOPIC,
        key=event.product_id,
        value=json.dumps(payload),
        callback=delivery_report,
    )

    producer.flush()


if __name__ == "__main__":
    event = ProductEvent(
        event_id="E001",
        event_type="product_updated",
        product_id="P001",
        product_name="Laptop",
        price=3500,
        quantity=2,
        region="Riyadh",
        event_time="2026-09-08T12:00:00",
    )

    publish_product_event(event)
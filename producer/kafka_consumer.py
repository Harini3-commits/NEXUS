
import json

from confluent_kafka import Consumer, KafkaException

from producer.validator import validate_event
from producer.deduplication import (
    initialize_database,
    process_event,
    get_revenue,
)


# 1. Configuration du consumer Kafka
config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "nexus-sqlite-consumer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

consumer = Consumer(config)

TOPIC = "nexus.orders"


def main():

    # 2. Initialisation de SQLite
    initialize_database()

    print("NEXUS KAFKA-SQLITE CONSUMER STARTED")
    print("Waiting for messages...\n")

    consumer.subscribe([TOPIC])

    try:
        while True:

            # 3. Lecture d'un message Kafka
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                raise KafkaException(message.error())

            try:
                # 4. Décodage JSON
                event = json.loads(
                    message.value().decode("utf-8")
                )

                # 5. Validation du contrat
                validate_event(event)

                event_id = event["event_id"]
                amount = event["total_amount"]

                # 6. Traitement idempotent dans SQLite
                processed = process_event(event_id, amount)

                if processed:
                    print(f"PROCESSED: {event_id}")
                    print(f"Revenue: {get_revenue():.2f} EUR")

                else:
                    print(f"DUPLICATE IGNORED: {event_id}")

                # 7. Commit Kafka après le traitement SQLite
                consumer.commit(
                    message=message,
                    asynchronous=False
                )

            except (ValueError, json.JSONDecodeError) as error:
                print(f"EVENT REJECTED: {error}")
                raise

    except KeyboardInterrupt:
        print("\nStopping consumer...")

    finally:
        consumer.close()
        print("NEXUS CONSUMER STOPPED")


if __name__ == "__main__":
    main()

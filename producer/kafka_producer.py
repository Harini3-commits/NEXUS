
import json
import time

from confluent_kafka import Producer
from producer.event_generator import generate_event


# 1. Configuration de la connexion Kafka
config = {
    "bootstrap.servers": "localhost:9092"
}

producer = Producer(config)

TOPIC = "nexus.orders"


# 2. Confirmation de livraison d'un message
def delivery_report(error, message):

    if error is not None:
        print(f"DELIVERY FAILED: {error}")

    else:
        print(
            f"DELIVERED: topic={message.topic()} "
            f"partition={message.partition()} "
            f"offset={message.offset()}"
        )


# 3. Envoi continu des événements
if __name__ == "__main__":

    print("NEXUS KAFKA PRODUCER STARTED")
    print("Press CTRL+C to stop\n")

    try:
        while True:

            event = generate_event()

            message = json.dumps(event)

            producer.produce(
                TOPIC,
                key=event["event_id"],
                value=message,
                callback=delivery_report
            )

            producer.poll(0)

            print(f"EVENT SENT: {event['event_id']}")

            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping producer...")

    finally:
        producer.flush()
        print("NEXUS KAFKA PRODUCER STOPPED")

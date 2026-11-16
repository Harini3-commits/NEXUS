
import json

from confluent_kafka import Consumer, KafkaException


# 1. Configuration du consumer
config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "nexus-consumer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
}

consumer = Consumer(config)

TOPIC = "nexus.orders"


# 2. Connexion au topic
consumer.subscribe([TOPIC])


if __name__ == "__main__":

    print("NEXUS KAFKA CONSUMER STARTED")
    print("Waiting for messages... Press CTRL+C to stop\n")

    try:
        while True:

            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                raise KafkaException(message.error())

            # 3. Décodage du message JSON
            event = json.loads(message.value().decode("utf-8"))

            print(
                f"RECEIVED: partition={message.partition()} "
                f"offset={message.offset()}"
            )

            print(json.dumps(event, indent=2, ensure_ascii=False))
            print("-" * 50)

            # 4. Confirmation de traitement
            consumer.commit(message=message, asynchronous=False)

    except KeyboardInterrupt:
        print("\nStopping consumer...")

    finally:
        consumer.close()
        print("NEXUS KAFKA CONSUMER STOPPED")

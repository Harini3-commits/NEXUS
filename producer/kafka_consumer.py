import base64
import json
from datetime import datetime, timezone


from confluent_kafka import Consumer, Producer, KafkaException


from producer.validator import validate_event
from producer.deduplication import (
    initialize_database,
    process_event,
    get_revenue,
)



# 1. Configuration Kafka Consumer
consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "nexus-sqlite-consumer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}


consumer = Consumer(consumer_config)



# 2. Configuration du producteur DLQ
dlq_producer = Producer({
    "bootstrap.servers": "localhost:9092"
})


SOURCE_TOPIC = "nexus.orders"
DLQ_TOPIC = "nexus.orders.dlq"



# 3. Construction du document JSON destiné à la DLQ
def build_dlq_event(message, error):
    """Construit le document JSON destiné à la DLQ."""
    raw_value = message.value() or b""
    original_key = message.key()

    if isinstance(original_key, bytes):
        original_key = original_key.decode(
            "utf-8", errors="replace"
        )

    return {
        "error": str(error),
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "topic": message.topic(),
            "partition": message.partition(),
            "offset": message.offset(),
        },
        "original_key": original_key,
        "original_value_base64": base64.b64encode(
            raw_value
        ).decode("ascii"),
    }



# 4. Envoi d'un événement invalide dans la DLQ
def publish_to_dlq(message, error):


    raw_value = message.value() or b""
    original_key = message.key()


    if isinstance(original_key, bytes):
        original_key = original_key.decode(
            "utf-8",
            errors="replace"
        )


    dlq_event = {
        "error": str(error),
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "topic": message.topic(),
            "partition": message.partition(),
            "offset": message.offset(),
        },
        "original_key": original_key,
        "original_value_base64": base64.b64encode(
            raw_value
        ).decode("ascii"),
    }


    delivery_status = {
        "error": None
    }


    def delivery_report(err, delivered_message):
        delivery_status["error"] = err


    # Clé déterministe : topic + partition + offset
    dlq_key = (
        f"{message.topic()}:"
        f"{message.partition()}:"
        f"{message.offset()}"
    )


    dlq_producer.produce(
        DLQ_TOPIC,
        key=dlq_key,
        value=json.dumps(dlq_event),
        callback=delivery_report
    )


    remaining = dlq_producer.flush(10)


    if remaining != 0:
        raise RuntimeError(
            "DLQ delivery timeout: message not confirmed"
        )


    if delivery_status["error"] is not None:
        raise RuntimeError(
            f"DLQ delivery failed: {delivery_status['error']}"
        )


    print(
        f"DLQ PUBLISHED: "
        f"source_offset={message.offset()}"
    )



# 5. Traitement principal
def main():


    initialize_database()


    consumer.subscribe([SOURCE_TOPIC])


    print("NEXUS KAFKA-SQLITE CONSUMER STARTED")
    print("DLQ enabled")
    print("Waiting for messages...\n")


    try:
        while True:


            message = consumer.poll(1.0)


            if message is None:
                continue


            if message.error():
                raise KafkaException(message.error())


            # 6. Décodage et validation des données
            try:
                event = json.loads(
                    message.value().decode("utf-8")
                )


                validate_event(event)


            except (
                UnicodeDecodeError,
                json.JSONDecodeError,
                ValueError,
            ) as error:


                print(f"INVALID EVENT: {error}")


                # On publie d'abord dans la DLQ
                publish_to_dlq(message, error)


                # On valide l'offset seulement après confirmation
                consumer.commit(
                    message=message,
                    asynchronous=False
                )


                print("INVALID EVENT ACKNOWLEDGED")
                continue


            # 7. Traitement SQLite
            event_id = event["event_id"]
            amount = event["total_amount"]


            processed = process_event(event_id, amount)


            if processed:
                print(f"PROCESSED: {event_id}")
                print(f"Revenue: {get_revenue():.2f} EUR")


            else:
                print(f"DUPLICATE IGNORED: {event_id}")


            # 8. Commit après traitement SQLite
            consumer.commit(
                message=message,
                asynchronous=False
            )


    except KeyboardInterrupt:
        print("\nStopping consumer...")


    finally:
        dlq_producer.flush(10)
        consumer.close()
        print("NEXUS CONSUMER STOPPED")



if __name__ == "__main__":
    main()

import base64
from datetime import datetime

from producer.kafka_consumer import build_dlq_event


class FakeMessage:
    def value(self):
        return b'{"event_id":"evt_bad_001","quantity":-5}'

    def key(self):
        return b"evt_bad_001"

    def topic(self):
        return "nexus.orders"

    def partition(self):
        return 2

    def offset(self):
        return 56


def test_build_dlq_event_preserves_source_information():
    message = FakeMessage()

    result = build_dlq_event(
        message,
        ValueError("Invalid quantity")
    )

    assert result["error"] == "Invalid quantity"
    assert result["source"]["topic"] == "nexus.orders"
    assert result["source"]["partition"] == 2
    assert result["source"]["offset"] == 56
    assert result["original_key"] == "evt_bad_001"


def test_build_dlq_event_preserves_original_payload():
    message = FakeMessage()

    result = build_dlq_event(
        message,
        ValueError("Invalid quantity")
    )

    decoded = base64.b64decode(
        result["original_value_base64"]
    )

    assert decoded == message.value()


def test_build_dlq_event_has_valid_timestamp():
    message = FakeMessage()

    result = build_dlq_event(
        message,
        ValueError("Invalid quantity")
    )

    timestamp = datetime.fromisoformat(
        result["failed_at"]
    )

    assert timestamp.tzinfo is not None

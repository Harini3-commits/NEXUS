
import pytest

from producer.validator import validate_event


# Événement de référence : il doit être accepté
VALID_EVENT = {
    "event_id": "evt_000001",
    "event_type": "order_created",
    "event_time": "2026-11-16T10:30:00Z",
    "customer_id": "C1024",
    "product_id": "P501",
    "quantity": 2,
    "unit_price": 49.99,
    "total_amount": 99.98,
    "currency": "EUR"
}


def test_valid_event_is_accepted():
    assert validate_event(VALID_EVENT) is True


def test_missing_required_field_is_rejected():
    event = VALID_EVENT.copy()
    del event["customer_id"]

    with pytest.raises(ValueError):
        validate_event(event)


def test_negative_quantity_is_rejected():
    event = VALID_EVENT.copy()
    event["quantity"] = -2

    with pytest.raises(ValueError):
        validate_event(event)


def test_incorrect_total_amount_is_rejected():
    event = VALID_EVENT.copy()
    event["total_amount"] = 150.00

    with pytest.raises(ValueError):
        validate_event(event)


def test_invalid_date_is_rejected():
    event = VALID_EVENT.copy()
    event["event_time"] = "not-a-date"

    with pytest.raises(ValueError):
        validate_event(event)


from producer.event_generator import generate_event
from producer.validator import validate_event


def test_generated_event_is_valid():
    event = generate_event()

    assert validate_event(event) is True


def test_generated_event_has_unique_id():
    events = [generate_event() for _ in range(100)]

    event_ids = [event["event_id"] for event in events]

    assert len(event_ids) == len(set(event_ids))


def test_generated_event_has_positive_quantity():
    event = generate_event()

    assert event["quantity"] >= 1


def test_generated_event_has_consistent_total():
    event = generate_event()

    expected_total = round(
        event["quantity"] * event["unit_price"], 2
    )

    assert round(event["total_amount"], 2) == expected_total

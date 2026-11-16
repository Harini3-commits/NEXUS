
import json
import time
import random
import uuid
from datetime import datetime, timezone
from decimal import Decimal

from producer.validator import validate_event


# Catalogue fictif de produits
PRODUCTS = {
    "P001": Decimal("19.99"),
    "P002": Decimal("49.99"),
    "P003": Decimal("89.90"),
    "P004": Decimal("129.00"),
    "P005": Decimal("9.99"),
}


def generate_event():
    """Génère une transaction e-commerce valide."""

    product_id = random.choice(list(PRODUCTS.keys()))
    unit_price = PRODUCTS[product_id]
    quantity = random.randint(1, 5)

    total_amount = (
        unit_price * quantity
    ).quantize(Decimal("0.01"))

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "order_created",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "customer_id": f"C{random.randint(1000, 9999)}",
        "product_id": product_id,
        "quantity": quantity,
        "unit_price": float(unit_price),
        "total_amount": float(total_amount),
        "currency": "EUR",
    }

    validate_event(event)

    return event



if __name__ == "__main__":
    print("NEXUS EVENT GENERATOR STARTED")
    print("Press CTRL+C to stop\n")

    try:
        while True:
            event = generate_event()

            print(json.dumps(event, ensure_ascii=False))

            time.sleep(1)

    except KeyboardInterrupt:
        print("\nNEXUS EVENT GENERATOR STOPPED")


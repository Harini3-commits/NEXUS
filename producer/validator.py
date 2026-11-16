
import json
from pathlib import Path
from decimal import Decimal

from jsonschema import Draft202012Validator, FormatChecker


# 1. Localisation du contrat de données
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_PATH = PROJECT_ROOT / "schemas" / "event.schema.json"


# 2. Chargement du contrat
def load_schema():
    with open(SCHEMA_PATH, "r", encoding="utf-8") as file:
        schema = json.load(file)

    Draft202012Validator.check_schema(schema)

    return schema


# 3. Validation d'un événement
def validate_event(event: dict) -> bool:

    schema = load_schema()

    validator = Draft202012Validator(
        schema,
        format_checker=FormatChecker()
    )

    errors = sorted(
        validator.iter_errors(event),
        key=lambda error: list(error.path)
    )

    if errors:
        messages = [
            f"{list(error.path)} : {error.message}"
            for error in errors
        ]
        raise ValueError(
            "Erreur de validation structurelle :\n"
            + "\n".join(messages)
        )

    # 4. Vérification d'une règle métier
    expected_total = (
        Decimal(str(event["quantity"]))
        * Decimal(str(event["unit_price"]))
    ).quantize(Decimal("0.01"))

    actual_total = Decimal(
        str(event["total_amount"])
    ).quantize(Decimal("0.01"))

    if expected_total != actual_total:
        raise ValueError(
            f"Montant incorrect : attendu {expected_total}, "
            f"reçu {actual_total}"
        )

    return True


# 5. Test du validateur
if __name__ == "__main__":

    test_event = {
        "event_id": "evt_000001",
        "event_type": "order_created",
        "event_time": "2026-11-03T10:30:00Z",
        "customer_id": "C1024",
        "product_id": "P501",
        "quantity": 2,
        "unit_price": 49.99,
        "total_amount": 99.98,
        "currency": "EUR"
    }

    try:
        validate_event(test_event)
        print("SUCCESS : Event is valid!")

    except ValueError as error:
        print("REJECTED :", error)

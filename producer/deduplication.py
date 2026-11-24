
import sqlite3
from pathlib import Path


# Chemin vers la base de données NEXUS
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "nexus.db"


def initialize_database():
    """Crée les tables nécessaires."""

    connection = sqlite3.connect(DB_PATH)

    try:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS processed_events (
                event_id TEXT PRIMARY KEY,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        connection.execute("""
            CREATE TABLE IF NOT EXISTS revenue (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                total_amount REAL NOT NULL DEFAULT 0
            )
        """)

        connection.execute("""
            INSERT OR IGNORE INTO revenue (id, total_amount)
            VALUES (1, 0)
        """)

        connection.commit()

    finally:
        connection.close()




def process_event(event_id: str, amount: float) -> bool:
    """Traite un événement une seule fois, de manière atomique."""

    connection = sqlite3.connect(DB_PATH)

    try:
        with connection:

            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO processed_events (event_id)
                VALUES (?)
                """,
                (event_id,)
            )

            if cursor.rowcount == 0:
                return False

            connection.execute(
                """
                UPDATE revenue
                SET total_amount = total_amount + ?
                WHERE id = 1
                """,
                (amount,)
            )

        return True

    finally:
        connection.close()




def get_revenue() -> float:
    """Retourne le chiffre d'affaires total."""

    connection = sqlite3.connect(DB_PATH)

    try:
        result = connection.execute(
            "SELECT total_amount FROM revenue WHERE id = 1"
        ).fetchone()

        return result[0]

    finally:
        connection.close()


# Test
if __name__ == "__main__":

    initialize_database()

    result = process_event("evt_final_001", 25.00)

    print("Event processed:", result)
    print("Total revenue:", get_revenue())

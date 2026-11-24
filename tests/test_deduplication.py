
import pytest

import producer.deduplication as deduplication


@pytest.fixture
def isolated_database(tmp_path, monkeypatch):
    """
    Crée une base SQLite temporaire pour chaque test.
    On ne touche donc jamais à la vraie base nexus.db.
    """

    test_db = tmp_path / "test_nexus.db"

    monkeypatch.setattr(
        deduplication,
        "DB_PATH",
        test_db
    )

    deduplication.initialize_database()

    return deduplication


def test_new_event_is_processed(isolated_database):
    result = isolated_database.process_event(
        "evt_001",
        49.99
    )

    assert result is True
    assert isolated_database.get_revenue() == pytest.approx(49.99)


def test_duplicate_event_is_ignored(isolated_database):
    isolated_database.process_event("evt_002", 49.99)

    result = isolated_database.process_event("evt_002", 49.99)

    assert result is False
    assert isolated_database.get_revenue() == pytest.approx(49.99)


def test_different_events_are_both_processed(isolated_database):
    first = isolated_database.process_event("evt_003", 20.00)
    second = isolated_database.process_event("evt_004", 30.00)

    assert first is True
    assert second is True
    assert isolated_database.get_revenue() == pytest.approx(50.00)


def test_duplicate_is_still_ignored_after_reinitialization(isolated_database):
    isolated_database.process_event("evt_005", 75.00)

    # Simule une nouvelle initialisation après un redémarrage
    isolated_database.initialize_database()

    result = isolated_database.process_event("evt_005", 75.00)

    assert result is False
    assert isolated_database.get_revenue() == pytest.approx(75.00)

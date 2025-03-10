import pytest
import os
import time
import pymysql
from pymysql.cursors import DictCursor
from stream import (
    BinLogProcessor,
    DatabaseConfig,
    InsertEvent,
    UpdateEvent,
    DeleteEvent,
    get_config_from_env,
)


@pytest.fixture
def test_db_config():
    return DatabaseConfig(
        user=os.environ.get("TEST_DB_USER", "root"),
        password=os.environ.get("TEST_DB_PASSWORD", "root"),
        host=os.environ.get("TEST_DB_HOST", "localhost"),
        database=os.environ.get("TEST_DB_NAME", "binlogstream"),
        charset="utf8mb4",
        collation="utf8mb4_general_ci",
    )


@pytest.fixture
def db_connection(test_db_config):
    config_dict = test_db_config.model_dump()
    connection = pymysql.connect(
        host=config_dict["host"],
        user=config_dict["user"],
        password=config_dict["password"],
        database=config_dict["database"],
        charset=config_dict["charset"],
        cursorclass=DictCursor,
    )

    yield connection

    connection.close()


@pytest.fixture
def setup_test_table(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS test_users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(100) NOT NULL
    )
    """)

    cursor.execute("DELETE FROM test_users")
    db_connection.commit()

    yield

    cursor.execute("DROP TABLE IF EXISTS test_users")
    db_connection.commit()
    cursor.close()


class TestDatabaseConfig:
    def test_valid_config(self, test_db_config):
        assert test_db_config.user is not None
        assert test_db_config.password is not None
        assert test_db_config.host is not None
        assert test_db_config.database is not None
        assert test_db_config.charset == "utf8mb4"
        assert test_db_config.collation == "utf8mb4_general_ci"


class TestBinLogProcessor:
    @pytest.fixture
    def processor(self, test_db_config):
        processor = BinLogProcessor(test_db_config)
        yield processor
        processor.close()

    def test_get_latest_binlog(self, processor):
        binlog = processor.get_latest_binlog()

        # We just check that we get a string back, not None
        # The actual value will depend on the database configuration
        assert binlog is not None
        assert isinstance(binlog, str)
        assert "bin" in binlog.lower()  # Most binary logs have "bin" in the name

    @pytest.mark.usefixtures("setup_test_table")
    def test_capture_insert_event(self, processor, db_connection):
        processor.connect()

        current_binlog = processor.get_latest_binlog()
        processor.log_file = current_binlog

        cursor = db_connection.cursor()
        cursor.execute(
            "INSERT INTO test_users (name, email) VALUES (%s, %s)",
            ("John Doe", "john@example.com"),
        )
        db_connection.commit()

        time.sleep(1)

        processor.close()
        processor.connect()

        events = []
        event_count = 0
        for event in processor.process_events():
            events.append(event)
            event_count += 1
            if event_count >= 10:  # Limit to avoid infinite loop
                break
            if isinstance(event, InsertEvent) and event.table == "test_users":
                break

        insert_events = [
            e for e in events if isinstance(e, InsertEvent) and e.table == "test_users"
        ]

        assert len(insert_events) > 0
        insert_event = insert_events[0]
        assert insert_event.type == "insert"
        assert insert_event.table == "test_users"

        assert "values" in insert_event.data
        assert "name" in insert_event.data["values"]
        assert insert_event.data["values"]["name"] == "John Doe"
        assert "email" in insert_event.data["values"]
        assert insert_event.data["values"]["email"] == "john@example.com"

    @pytest.mark.usefixtures("setup_test_table")
    def test_capture_update_event(self, processor, db_connection):
        cursor = db_connection.cursor()
        cursor.execute(
            "INSERT INTO test_users (name, email) VALUES (%s, %s)",
            ("Jane Doe", "jane@example.com"),
        )
        db_connection.commit()

        cursor.execute("SELECT id FROM test_users WHERE name = 'Jane Doe'")
        user_id = cursor.fetchone()["id"]

        processor.connect()

        cursor.execute(
            "UPDATE test_users SET name = %s, email = %s WHERE id = %s",
            ("Jane Smith", "jane.smith@example.com", user_id),
        )
        db_connection.commit()

        time.sleep(1)

        processor.close()
        processor.connect()

        events = []
        event_count = 0
        for event in processor.process_events():
            events.append(event)
            event_count += 1
            if event_count >= 10:  # Limit to avoid infinite loop
                break
            if isinstance(event, UpdateEvent) and event.table == "test_users":
                break

        update_events = [
            e for e in events if isinstance(e, UpdateEvent) and e.table == "test_users"
        ]

        assert len(update_events) > 0
        update_event = update_events[0]
        assert update_event.type == "update"
        assert update_event.table == "test_users"

        print(f"Update event data structure: {update_event}")

        assert "name" in update_event.before
        assert update_event.before["name"] == "Jane Doe"
        assert "email" in update_event.before
        assert update_event.before["email"] == "jane@example.com"

        assert "name" in update_event.after
        assert update_event.after["name"] == "Jane Smith"
        assert "email" in update_event.after
        assert update_event.after["email"] == "jane.smith@example.com"

    @pytest.mark.usefixtures("setup_test_table")
    def test_capture_delete_event(self, processor, db_connection):
        cursor = db_connection.cursor()
        cursor.execute(
            "INSERT INTO test_users (name, email) VALUES (%s, %s)",
            ("Bob Johnson", "bob@example.com"),
        )
        db_connection.commit()

        cursor.execute("SELECT id FROM test_users WHERE name = 'Bob Johnson'")
        user_id = cursor.fetchone()["id"]

        processor.connect()

        cursor.execute("DELETE FROM test_users WHERE id = %s", (user_id,))
        db_connection.commit()

        time.sleep(1)

        processor.close()
        processor.connect()

        events = []
        event_count = 0
        for event in processor.process_events():
            events.append(event)
            event_count += 1
            if event_count >= 10:  # Limit to avoid infinite loop
                break
            if isinstance(event, DeleteEvent) and event.table == "test_users":
                break

        delete_events = [
            e for e in events if isinstance(e, DeleteEvent) and e.table == "test_users"
        ]

        assert len(delete_events) > 0
        delete_event = delete_events[0]
        assert delete_event.type == "delete"
        assert delete_event.table == "test_users"

        assert "values" in delete_event.data
        assert "name" in delete_event.data["values"]
        assert delete_event.data["values"]["name"] == "Bob Johnson"
        assert "email" in delete_event.data["values"]
        assert delete_event.data["values"]["email"] == "bob@example.com"


@pytest.mark.skipif(
    not all(
        [
            os.environ.get("DB_USER"),
            os.environ.get("DB_PASSWORD"),
            os.environ.get("DB_HOST"),
            os.environ.get("DB_NAME"),
        ]
    ),
    reason="Environment variables for database not set",
)
def test_get_config_from_env():
    config = get_config_from_env()

    assert config.user == os.environ.get("DB_USER")
    assert config.password == os.environ.get("DB_PASSWORD")
    assert config.host == os.environ.get("DB_HOST")
    assert config.database == os.environ.get("DB_NAME")
    assert config.charset == "utf8mb4"
    assert config.collation == "utf8mb4_general_ci"

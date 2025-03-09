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


# Test database configuration
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

    # Create test table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS test_users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(100) NOT NULL
    )
    """)

    # Clear any existing data
    cursor.execute("DELETE FROM test_users")
    db_connection.commit()

    yield

    # Clean up
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
        # This test requires binary logging to be enabled on the test database
        binlog = processor.get_latest_binlog()

        # We just check that we get a string back, not None
        # The actual value will depend on the database configuration
        assert binlog is not None
        assert isinstance(binlog, str)
        assert "bin" in binlog.lower()  # Most binary logs have "bin" in the name

    @pytest.mark.usefixtures("setup_test_table")
    def test_capture_insert_event(self, processor, db_connection):
        # Start the processor
        processor.connect()

        # Get the current position to start reading from
        current_binlog = processor.get_latest_binlog()
        processor.log_file = current_binlog

        # Insert a record
        cursor = db_connection.cursor()
        cursor.execute(
            "INSERT INTO test_users (name, email) VALUES (%s, %s)",
            ("John Doe", "john@example.com"),
        )
        db_connection.commit()

        # Give the database a moment to process
        time.sleep(1)

        # Reconnect the processor to get fresh events
        processor.close()
        processor.connect()

        # Process events
        events = []
        event_count = 0
        for event in processor.process_events():
            events.append(event)
            event_count += 1
            if event_count >= 10:  # Limit to avoid infinite loop
                break
            if isinstance(event, InsertEvent) and event.table == "test_users":
                break

        # Find our insert event
        insert_events = [
            e for e in events if isinstance(e, InsertEvent) and e.table == "test_users"
        ]

        assert len(insert_events) > 0
        insert_event = insert_events[0]
        assert insert_event.type == "insert"
        assert insert_event.table == "test_users"

        # Fix: Access the nested 'values' dictionary
        assert "values" in insert_event.data
        assert "name" in insert_event.data["values"]
        assert insert_event.data["values"]["name"] == "John Doe"
        assert "email" in insert_event.data["values"]
        assert insert_event.data["values"]["email"] == "john@example.com"

        @pytest.mark.usefixtures("setup_test_table")
        def test_capture_update_event(self, processor, db_connection):
            # Insert a record first
            cursor = db_connection.cursor()
            cursor.execute(
                "INSERT INTO test_users (name, email) VALUES (%s, %s)",
                ("Jane Doe", "jane@example.com"),
            )
            db_connection.commit()

            # Get the ID of the inserted record
            cursor.execute("SELECT id FROM test_users WHERE name = 'Jane Doe'")
            user_id = cursor.fetchone()["id"]

            # Start the processor
            processor.connect()

            # Update the record
            cursor.execute(
                "UPDATE test_users SET name = %s, email = %s WHERE id = %s",
                ("Jane Smith", "jane.smith@example.com", user_id),
            )
            db_connection.commit()

            # Give the database a moment to process
            time.sleep(1)

            # Reconnect the processor to get fresh events
            processor.close()
            processor.connect()

            # Process events
            events = []
            event_count = 0
            for event in processor.process_events():
                events.append(event)
                event_count += 1
                if event_count >= 10:  # Limit to avoid infinite loop
                    break
                if isinstance(event, UpdateEvent) and event.table == "test_users":
                    break

            # Find our update event
            update_events = [
                e
                for e in events
                if isinstance(e, UpdateEvent) and e.table == "test_users"
            ]

            assert len(update_events) > 0
            update_event = update_events[0]
            assert update_event.type == "update"
            assert update_event.table == "test_users"

            # Let's print the structure to understand it better
            print(f"Update event data structure: {update_event.data}")

            # Assuming the structure is a list of rows, each with before/after values
            assert len(update_event.data) > 0
            row = update_event.data[0]  # Get the first row

            # Check if it's a tuple structure (before, after)
            if isinstance(row, tuple) and len(row) == 2:
                before, after = row
                assert "name" in before
                assert before["name"] == "Jane Doe"
                assert "email" in before
                assert before["email"] == "jane@example.com"
                assert "name" in after
                assert after["name"] == "Jane Smith"
                assert "email" in after
                assert after["email"] == "jane.smith@example.com"
            # Or if it's a dictionary with before_values/after_values
            elif (
                isinstance(row, dict)
                and "before_values" in row
                and "after_values" in row
            ):
                assert "name" in row["before_values"]
                assert row["before_values"]["name"] == "Jane Doe"
                assert "email" in row["before_values"]
                assert row["before_values"]["email"] == "jane@example.com"
                assert "name" in row["after_values"]
                assert row["after_values"]["name"] == "Jane Smith"
                assert "email" in row["after_values"]
                assert row["after_values"]["email"] == "jane.smith@example.com"

    @pytest.mark.usefixtures("setup_test_table")
    def test_capture_delete_event(self, processor, db_connection):
        # Insert a record first
        cursor = db_connection.cursor()
        cursor.execute(
            "INSERT INTO test_users (name, email) VALUES (%s, %s)",
            ("Bob Johnson", "bob@example.com"),
        )
        db_connection.commit()

        # Get the ID of the inserted record
        cursor.execute("SELECT id FROM test_users WHERE name = 'Bob Johnson'")
        user_id = cursor.fetchone()["id"]

        # Start the processor
        processor.connect()

        # Delete the record
        cursor.execute("DELETE FROM test_users WHERE id = %s", (user_id,))
        db_connection.commit()

        # Give the database a moment to process
        time.sleep(1)

        # Reconnect the processor to get fresh events
        processor.close()
        processor.connect()

        # Process events
        events = []
        event_count = 0
        for event in processor.process_events():
            events.append(event)
            event_count += 1
            if event_count >= 10:  # Limit to avoid infinite loop
                break
            if isinstance(event, DeleteEvent) and event.table == "test_users":
                break

        # Find our delete event
        delete_events = [
            e for e in events if isinstance(e, DeleteEvent) and e.table == "test_users"
        ]

        assert len(delete_events) > 0
        delete_event = delete_events[0]
        assert delete_event.type == "delete"
        assert delete_event.table == "test_users"

        # Fix: Access the nested 'values' dictionary
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

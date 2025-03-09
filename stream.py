# Map these events from a binlog file

# === WriteRowsEvent ===
# Date: 2024-12-09T19:14:02
# Log position: 0
# Event size: 39
# Read bytes: 10
# Table: binlog_demo_db.users
# Affected columns: 3
# Changed rows: 1
# Column Name Information Flag: True
# Values:
# --
# * id : 4
# * name : John
# * email : john@example.com

# === UpdateRowsEvent ===
# Date: 2024-12-09T19:15:11
# Log position: 0
# Event size: 65
# Read bytes: 11
# Table: binlog_demo_db.users
# Affected columns: 3
# Changed rows: 1
# Column Name Information Flag: True
# Values:
# --
# *id:3=>3
# *name:Mai=>Mei
# *email:mai@example.com=>mei@example.com

# === DeleteRowsEvent ===
# Date: 2024-12-09T19:23:15
# Log position: 0
# Event size: 39
# Read bytes: 10
# Table: binlog_demo_db.users
# Affected columns: 3
# Changed rows: 1
# Column Name Information Flag: True
# Values:
# --
# * id : 4
# * name : John
# * email : john@example.com

import os
import argparse
from typing import Dict, Optional, Any, Iterator, Union
from dotenv import load_dotenv
import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent,
)
from pydantic import BaseModel, field_validator


class DatabaseConfig(BaseModel):
    user: str
    password: str
    host: str
    database: str
    charset: str = "utf8mb4"
    collation: str = "utf8mb4_general_ci"

    @field_validator("host")
    def validate_host(cls, v):
        if not v:
            raise ValueError("Host cannot be empty")
        return v


class InsertEvent(BaseModel):
    type: str = "insert"
    table: str
    data: Dict[str, Any]


class UpdateEvent(BaseModel):
    type: str = "update"
    table: str
    before: Dict[str, Any]
    after: Dict[str, Any]


class DeleteEvent(BaseModel):
    type: str = "delete"
    table: str
    data: Dict[str, Any]


Event = Union[InsertEvent, UpdateEvent, DeleteEvent]


class BinLogProcessor:
    def __init__(
        self, config: DatabaseConfig, server_id: int = 1, log_file: Optional[str] = None
    ):
        self.config = config
        self.server_id = server_id
        self.log_file = log_file
        self.stream = None

    def connect(self) -> None:
        stream_args = {
            "connection_settings": self.config.model_dump(),
            "server_id": self.server_id,
        }

        if self.log_file:
            stream_args["log_file"] = self.log_file

        self.stream = BinLogStreamReader(**stream_args)

    def get_latest_binlog(self) -> Optional[str]:
        config_dict = self.config.model_dump()
        connection = pymysql.connect(
            host=config_dict["host"],
            user=config_dict["user"],
            password=config_dict["password"],
            database=config_dict["database"],
            charset=config_dict.get("charset", "utf8mb4"),
        )

        try:
            with connection.cursor() as cursor:
                cursor.execute("SHOW BINARY LOGS")
                logs = cursor.fetchall()
                if logs:
                    # Return the most recent log file (last in the list)
                    return logs[-1][0]
                return None
        finally:
            connection.close()

    def process_events(self) -> Iterator[Event]:
        if not self.stream:
            self.connect()

        for event in self.stream:
            if isinstance(event, WriteRowsEvent):
                for row in event.rows:
                    yield InsertEvent(table=event.table, data=row)
            elif isinstance(event, UpdateRowsEvent):
                for row in event.rows:
                    print(row)
                    yield UpdateEvent(
                        table=event.table,
                        before=row["before_values"],
                        after=row["after_values"],
                    )
            elif isinstance(event, DeleteRowsEvent):
                for row in event.rows:
                    yield DeleteEvent(table=event.table, data=row)

    def close(self) -> None:
        if self.stream:
            self.stream.close()
            self.stream = None


def get_config_from_env() -> DatabaseConfig:
    load_dotenv()

    return DatabaseConfig(
        user=os.environ.get("DB_USER", ""),
        password=os.environ.get("DB_PASSWORD", ""),
        host=os.environ.get("DB_HOST", ""),
        database=os.environ.get("DB_NAME", ""),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Process MySQL binary logs")
    parser.add_argument("--log-file", help="Specific binary log file to read from")
    parser.add_argument(
        "--server-id", type=int, default=1, help="Server ID for the binary log reader"
    )
    args = parser.parse_args()

    try:
        config = get_config_from_env()
        processor = BinLogProcessor(
            config, server_id=args.server_id, log_file=args.log_file
        )

        for event in processor.process_events():
            if isinstance(event, InsertEvent):
                print(f"Insert into {event.table}:")
                print(event.data)
            elif isinstance(event, UpdateEvent):
                print(f"Update in {event.table}:")
                print(f"Before: {event.before}")
                print(f"After: {event.after}")
            elif isinstance(event, DeleteEvent):
                print(f"Delete from {event.table}:")
                print(event.data)
            print("---")
    except KeyboardInterrupt:
        print("Stopping binary log processor...")
    finally:
        if "processor" in locals():
            processor.close()


if __name__ == "__main__":
    main()

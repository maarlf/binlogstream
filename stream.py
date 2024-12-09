from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent


if __name__ == "__main__":
  # MySQL connection parameters
  config = {
    'user': '<user>',
    'password': '<password>',
    'host': '<host>',
    'database': '<db>',
    'charset':'utf8mb4',
    'collation':'utf8mb4_general_ci'  # Specify a supported collation
  }

  stream = BinLogStreamReader(connection_settings=config, server_id=1, log_file='mariadb.000001')

  # Map these events fron a binlog file

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

  for binlogevent in stream:
    if isinstance(binlogevent, WriteRowsEvent):
      print('Insert')
      print(binlogevent.rows)
    elif isinstance(binlogevent, UpdateRowsEvent):
      print('Update')
      print(binlogevent.rows)
    elif isinstance(binlogevent, DeleteRowsEvent):
      print('Delete')
      print(binlogevent.rows)

  stream.close()

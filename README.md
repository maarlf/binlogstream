# binlogstream

MySQL/MariaDB Binlog stream with Python.

## Usage

1. Create virtual environment with Python 3

2. Install dependencies

```
  pip install -r requirements.txt
```

3. Change placeholders for the connection and `BinlogStreamReader` config in `stream.py`

4. Make some changes in the database

5. See events with `python stream.py`

## Binlog Tools

You can use built-in MySQL or MariaDB tools to read and manage binlog files:

- Show Binary Logs: To see the list of binary logs, you can run:

```
  SHOW BINARY LOGS;
```

- Show Current Binlog File: To find out which binlog file is currently being written to:

```
  SHOW MASTER STATUS;
```

- Read Binlog Events: To read the contents of a specific binlog file, use:

```
  SHOW BINLOG EVENTS IN 'mysql-bin.000001';
```


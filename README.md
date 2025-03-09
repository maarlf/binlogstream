# binlogstream

MySQL/MariaDB Binlog stream with Python.

## Requirements

- Python 3
- Docker and Docker Compose

## Usage

1. Create virtual environment with Python 3

2. Install dependencies

```
  pip install -r requirements.txt
```

3. Prepare the .env files containing these contents

```
DB_NAME=
DB_USER=
DB_PASSWORD=
DB_ROOT_PASSWORD=
DB_HOST=
```

4. Setup the database instance with `docker compose up -d` then `./setup.sh` and see `init-scripts/` for the detailed database setup

5. Make some changes in the database

6. See events with `python stream.py`

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


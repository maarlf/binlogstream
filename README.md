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

from pymysqlreplication import BinLogStreamReader

# MySQL connection parameters
config = {
  'user': '<user>',
  'password': '<secret>',
  'host': '<host>',
  'database': '<db>',
  'charset':'utf8mb4',
  'collation':'utf8mb4_general_ci'  # Specify a supported collation
}

stream = BinLogStreamReader(connection_settings = config, server_id=1)

for binlogevent in stream:
  binlogevent.dump()

stream.close()

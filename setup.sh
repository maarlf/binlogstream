#!/bin/bash
# Read username from .env or prompt user
source .env 2>/dev/null || true
DB_USER=${DB_USER:-"example_user"}

# Generate SQL file from template
sed "s/{{DB_USER}}/$DB_USER/g" init-scripts/01-grant-binlog-privileges.sql.template > init-scripts/01-grant-binlog-privileges.sql

echo "SQL file generated with username: $DB_USER"

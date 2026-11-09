#!/bin/sh
# Runs once, on first start of the primary, after init.sql.
set -eu

psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD '$POSTGRES_PASSWORD'"
echo "host replication replicator all scram-sha-256" >> "$PGDATA/pg_hba.conf"

# Written to the file, not passed with -c: the temporary init server must not wait for a
# standby that does not exist yet. A commit is acknowledged only after the standby has it.
cat >> "$PGDATA/postgresql.conf" <<CONF
wal_keep_size = 256MB
synchronous_standby_names = 'standby1'
CONF

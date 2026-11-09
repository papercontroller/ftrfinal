#!/bin/sh
# Hot standby of the primary, plus the watchdog that promotes it. Runs as postgres.
set -eu

PGDATA=/var/lib/postgresql/data
export PGPASSWORD="$DB_PASSWORD"

if [ ! -s "$PGDATA/PG_VERSION" ]; then
  until pg_isready -q -h db; do sleep 1; done
  pg_basebackup -h db -U replicator -D "$PGDATA" -X stream -R
  # The name must match synchronous_standby_names on the primary.
  echo "primary_conninfo = 'host=db user=replicator password=$DB_PASSWORD application_name=standby1'" >> "$PGDATA/postgresql.auto.conf"
  chmod 700 "$PGDATA"
fi

# Experiments that test the database fault itself switch this off with
# `touch /tmp/no-failover`, so a freeze stays a freeze.
watchdog() {
  until pg_isready -q; do sleep 1; done
  [ "$(psql -U uis_user -d uis_db -Atc 'SELECT pg_is_in_recovery()')" = t ] || return 0
  fails=0
  while :; do
    sleep 1
    # timeout: once the container is gone, the name lookup alone can take seconds.
    if [ -f /tmp/no-failover ] || timeout 1 pg_isready -q -t 1 -h db; then
      fails=0
      continue
    fi
    fails=$((fails + 1))
    echo "watchdog: primary check failed ($fails/5)"
    if [ "$fails" -ge 5 ]; then
      pg_ctl promote -D "$PGDATA"
      # There is no standby of this node yet. Keeping the setting would hang every commit.
      psql -U uis_user -d uis_db -c "ALTER SYSTEM SET synchronous_standby_names = ''" -c "SELECT pg_reload_conf()"
      echo "watchdog: promoted"
      return 0
    fi
  done
}

watchdog &
exec postgres

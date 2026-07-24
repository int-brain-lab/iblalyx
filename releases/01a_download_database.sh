#!/bin/bash
set -e

# This script populates the local openalyx buffer database (container openalyx_buffer_postgres)
# with a copy of the production database, then migrates it.
# - DUMP_SOURCE=nightly (default): streams last night's flatiron backup, same dump 01_backup_ibl.sh
#   already produces. No production load beyond what that nightly cron already does.
# - DUMP_SOURCE=live: pg_dumps the production database directly, for when you need the most
#   recent data. Adds load/time to production at whatever moment you run this.
# Nothing is written to disk — both sources are piped straight into the buffer container.

# Source env variables
export $(grep -v '^#' ../docker/.env | xargs)

echo "$(date '+%Y-%m-%d %H:%M:%S') Beginning to populate buffer database"

echo "... resetting buffer database"
docker exec -i openalyx_buffer_postgres psql -q -U "$OPENALYX_BUFFER_DB_USER" -d "$OPENALYX_BUFFER_DB_NAME" \
  -c "drop schema public cascade; create schema public"

# The nightly flatiron dump is a plain pg_dump of production without --no-owner/--no-acl, so it's
# full of "ALTER ... OWNER TO ibl_dev" and "GRANT/REVOKE ... ibl_dev" statements. ibl_dev doesn't
# exist in this buffer, so without this role those statements error out (harmlessly, but noisily,
# and it's fragile to rely on psql tolerating them). Creating a role with the same name is enough:
# it needs no login/password, it's only ever referenced as an ownership/grant target here.
docker exec -i openalyx_buffer_postgres psql -q -U "$OPENALYX_BUFFER_DB_USER" -d "$OPENALYX_BUFFER_DB_NAME" \
  -c "create role ibl_dev;" 2>/dev/null || true

if [ "$DUMP_SOURCE" = "live" ]; then
    echo "... streaming a fresh dump directly from production (DUMP_SOURCE=live)"
    pg_dump -h "$PROD_DB_HOST" -p "$PROD_DB_PORT" -U "$PROD_DB_USER" -d "$PROD_DB_NAME" --no-owner --no-acl \
    | docker exec -i openalyx_buffer_postgres psql -q -U "$OPENALYX_BUFFER_DB_USER" -d "$OPENALYX_BUFFER_DB_NAME"
else
    echo "... streaming last night's production backup from flatiron (DUMP_SOURCE=nightly)"
    BACKUP_DATE=$(date -d 'today' +%Y-%m-%d)
    wget -O - --user="$WGET_USER" --password="$WGET_PASSWORD" \
      "https://ibl.flatironinstitute.org/json/${BACKUP_DATE}_alyxfull.sql.gz" \
    | gunzip \
    | docker exec -i openalyx_buffer_postgres psql -q -U "$OPENALYX_BUFFER_DB_USER" -d "$OPENALYX_BUFFER_DB_NAME"
fi

echo "... running migrations"
docker exec -i ibl_alyx_apache bash -c "python /var/www/alyx/alyx/manage.py migrate --database public"

echo "$(date '+%Y-%m-%d %H:%M:%S') Finished populating buffer database"

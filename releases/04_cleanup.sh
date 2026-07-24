#!/bin/bash
set -e

# Cleans up after a completed release:
# - drops the dated "_prev_" database that 02_upload_public_db.sh's rename-swap leaves behind
#   on the public RDS as a same-day rollback, once you've confirmed the release looks correct
# - drops and recreates the local openalyx buffer (container + volume), since it's fully
#   rebuilt from scratch by 01a_download_database.sh anyway and there's no reason to keep its
#   data on disk between releases

# Usage: ./04_cleanup.sh [YYYY-MM-DD]
# Defaults to today's date (the date 02_upload_public_db.sh's swap ran on, if run same-day).

# Source env variables
export $(grep -v '^#' ../docker/.env | xargs)
export PGPASSWORD="$OPENALYX_DB_PASSWORD"

DATE="${1:-$(date +%Y-%m-%d)}"
PREV_DB="${OPENALYX_DB_NAME}_prev_${DATE}"

if [ "$PREV_DB" = "$OPENALYX_DB_NAME" ]; then
    echo "Refusing to drop $OPENALYX_DB_NAME itself - check the date argument" >&2
    exit 1
fi

echo "$(date '+%Y-%m-%d %H:%M:%S') Beginning cleanup"

echo "... checking $PREV_DB exists on openalyx RDS"
EXISTS=$(psql -q -U "$OPENALYX_DB_USER" -h "$OPENALYX_DB_HOST" -p "$OPENALYX_DB_PORT" -d postgres -t -A \
  -c "select 1 from pg_database where datname = '$PREV_DB';")
if [ "$EXISTS" != "1" ]; then
    echo "$PREV_DB does not exist, skipping RDS cleanup" >&2
    echo "Existing ${OPENALYX_DB_NAME}_prev_* databases:" >&2
    psql -q -U "$OPENALYX_DB_USER" -h "$OPENALYX_DB_HOST" -p "$OPENALYX_DB_PORT" -d postgres -t -A \
      -c "select datname from pg_database where datname like '${OPENALYX_DB_NAME}_prev_%' order by datname;" >&2
else
    echo "... dropping $PREV_DB from openalyx RDS"
    psql -q -U "$OPENALYX_DB_USER" -h "$OPENALYX_DB_HOST" -p "$OPENALYX_DB_PORT" -d postgres \
      -c "select pg_terminate_backend(pid) from pg_stat_activity where datname = '$PREV_DB' and pid <> pg_backend_pid();" \
      -c "drop database \"$PREV_DB\";"
fi

# ... measuring local openalyx buffer volume size
BUFFER_SIZE=$(docker system df -v | awk '/docker_openalyx_buffer_postgres/{print $NF}')

echo "... dropping local openalyx buffer (container + volume) to free disk"
docker compose -f ../docker/docker-compose.yaml rm --stop --force openalyx_buffer_postgres
docker volume rm docker_openalyx_buffer_postgres
echo "... freed ${BUFFER_SIZE:-an unknown amount of} disk space"

echo "... recreating empty openalyx buffer, ready for the next release"
docker compose -f ../docker/docker-compose.yaml up -d openalyx_buffer_postgres

echo "$(date '+%Y-%m-%d %H:%M:%S') Finished cleanup"

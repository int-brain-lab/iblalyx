#!/bin/bash
set -e

# This script uploads the pruned buffer database to the real openalyx RDS using a rename-swap:
# it builds the release into a fresh database, then swaps it in for the live database by
# renaming, rather than dropping/reloading the live database in place. This keeps openalyx
# available right up until the swap itself (a few seconds of dropped connections), and leaves
# the previous database around under a dated name as an easy rollback.

# Consult the Alyx playbook before running this, as it requires some careful setup. Minimally:
# * Make sure you have ssh access to the openalyx EC2 instance to set the maintenance trigger (see the playbook) —
#   this script does that for you.
# * Make sure you have the password for the postgres user on the openalyx RDS instance (iblalyx/docker/.env).
# * Record the output of this script in a log file and submit these logs to github for the record.

# Source env variables
export $(grep -v '^#' ../docker/.env | xargs)
# psql looks for PGPASSWORD specifically, not OPENALYX_DB_PASSWORD - without this it prompts
# interactively and fails non-interactively with "no password supplied".
export PGPASSWORD="$OPENALYX_DB_PASSWORD"

NEW_DB="${OPENALYX_DB_NAME}_next"
PREV_DB="${OPENALYX_DB_NAME}_prev_$(date +%Y-%m-%d)"

echo "$(date '+%Y-%m-%d %H:%M:%S') Beginning upload to openalyx RDS"

echo "... backing up pruned buffer database to flatiron"
docker exec openalyx_buffer_postgres pg_dump -cOx -U "$OPENALYX_BUFFER_DB_USER" -d "$OPENALYX_BUFFER_DB_NAME" \
| gzip -5 \
| ssh -i /home/ubuntu/.ssh/sdsc_alyx.pem -p 62022 alyx@ibl-ssh.flatironinstitute.org \
    'set -e; target="/mnt/ibl/json/openalyx/'"$(date +%F)"'_openalyx.sql.gz"; part="${target}.part"; cat > "$part" && mv "$part" "$target"'

echo "... creating fresh database $NEW_DB on openalyx RDS"
psql -q -U "$OPENALYX_DB_USER" -h "$OPENALYX_DB_HOST" -p "$OPENALYX_DB_PORT" -d postgres \
  -c "drop database if exists \"$NEW_DB\";" \
  -c "create database \"$NEW_DB\" owner \"$OPENALYX_DB_USER\";"

echo "... loading pruned buffer database into $NEW_DB"
docker exec openalyx_buffer_postgres pg_dump -cOx -U "$OPENALYX_BUFFER_DB_USER" -d "$OPENALYX_BUFFER_DB_NAME" \
| psql -q -U "$OPENALYX_DB_USER" -h "$OPENALYX_DB_HOST" -p "$OPENALYX_DB_PORT" -d "$NEW_DB"

echo "... setting maintenance trigger on openalyx EC2"
ssh -o BatchMode=yes -o ConnectTimeout=10 openalyx "docker exec alyx_apache touch /var/www/alyx/maintenance.trigger"

echo "... swapping $NEW_DB in for $OPENALYX_DB_NAME (previous kept as $PREV_DB)"
psql -q -U "$OPENALYX_DB_USER" -h "$OPENALYX_DB_HOST" -p "$OPENALYX_DB_PORT" -d postgres <<SQL
select pg_terminate_backend(pid) from pg_stat_activity where datname in ('$OPENALYX_DB_NAME', '$NEW_DB') and pid <> pg_backend_pid();
alter database "$OPENALYX_DB_NAME" rename to "$PREV_DB";
alter database "$NEW_DB" rename to "$OPENALYX_DB_NAME";
SQL

echo "$(date '+%Y-%m-%d %H:%M:%S') Finished uploading database to openalyx RDS"
echo "... removing maintenance trigger on openalyx EC2"
ssh -o BatchMode=yes -o ConnectTimeout=10 openalyx "docker exec alyx_apache rm /var/www/alyx/maintenance.trigger"
echo "Previous database kept as $PREV_DB — drop it manually once you've confirmed the release looks correct."

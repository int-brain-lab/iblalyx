#!/bin/bash
set -e
# Checks that production alyx, openalyx and the local alyx checkout (the ibl_alyx_apache
# container used by the rest of these releases/ scripts) are all on the same git commit,
# and runs Django checks on local container databases.
# Run this before 01a_download_database.sh.

ALYX_REPO_PATH=/var/www/alyx/alyx

echo "... checking local environment (db access)"

# load environment variables from.env
export $(grep -v '^#' ../docker/.env | xargs)

# check the postgres connection for remote public databases
pg_isready -d $OPENALYX_DB_NAME -h $OPENALYX_DB_HOST -p $OPENALYX_DB_PORT -U $OPENALYX_DB_USER

# checking connection to database and settings from Django
echo "... public (local openalyx buffer db)"
docker exec ibl_alyx_apache python $ALYX_REPO_PATH/manage.py check --database public

echo "... default (production db)"
docker exec ibl_alyx_apache sh -c "python $ALYX_REPO_PATH/manage.py check && python $ALYX_REPO_PATH/manage.py showmigrations"

echo "... checking alyx git commit on alyx-prod, openalyx and local"
PROD_HASH=$(ssh -o BatchMode=yes -o ConnectTimeout=10 alyx-prod "docker exec alyx_apache git -C $ALYX_REPO_PATH rev-parse HEAD")
OPENALYX_HASH=$(ssh -o BatchMode=yes -o ConnectTimeout=10 openalyx "docker exec alyx_apache git -C $ALYX_REPO_PATH rev-parse HEAD")
LOCAL_HASH=$(docker exec ibl_alyx_apache git -C "$ALYX_REPO_PATH" rev-parse HEAD)

echo "alyx-prod: $PROD_HASH"
echo "openalyx:  $OPENALYX_HASH"
echo "local:     $LOCAL_HASH"

if [ "$PROD_HASH" = "$OPENALYX_HASH" ] && [ "$PROD_HASH" = "$LOCAL_HASH" ]; then
    echo "OK: all three are on the same commit"
else
    echo "MISMATCH: alyx-prod, openalyx and local are not all on the same commit" >&2
    exit 1
fi

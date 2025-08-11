#!/bin/bash
set -e

# This script creates a local version of the public database through the following steps:
# 1. Destroy the local instance of the public database
# 2. Load the latest sql dump of the production database into the local public database
# 3. Prune the local public database to contain only the to-be-released information

# Consult the Alyx playbook before running this, as it requires some careful setup. Minimally:
# * Make sure you have the correct database set up as 'public' in the alyx installation you use
# * Make sure all involved databases are on the same branch and commit (production, public, local install)
# * Record the output of this script in a log file (and submit these logs to github for the record later)
# * Adjust the variables in the .env file according to your setup (see the .env_template file for the variables)

# Source env variables and alyx env
export $(grep -v '^#' .env | xargs)
source $ALYX_VENV/bin/activate

# checking connection to database and settings
python $ALYX_DIR/alyx/manage.py check
python $ALYX_DIR/alyx/manage.py showmigrations

echo "$(date '+%Y-%m-%d %H:%M:%S') Beginning to create local version of public database"
# Create a working directory
echo "... creating working directory $TMP_DIR"
mkdir -p "$TMP_DIR"
# Copying and unzipping the most recent alyx backup into it, SSH_STR needs to be setup correctly above
echo "... copying latest backup of production database"
scp $SSH_STR:/backups/alyx-backups/$(date +%Y-%m-%d)/alyx_full.sql.gz "$TMP_DIR"/alyxfull.sql.gz
gunzip -f -c "$TMP_DIR"/alyxfull.sql.gz > "$TMP_DIR"/alyxfull.sql

# Destroy local public database and load production database into it
echo "... destroying local public database"
psql -q -U $BUFFER_DB_USER -h $BUFFER_DB_HOST -d $BUFFER_DB_NAME -p $BUFFER_DB_PORT -c "drop schema public cascade"
psql -q -U $BUFFER_DB_USER -h $BUFFER_DB_HOST -d $BUFFER_DB_NAME -p $BUFFER_DB_PORT -c "create schema public"
echo "... rebuilding local public database from production"
psql -q -U $BUFFER_DB_USER -h $BUFFER_DB_HOST -d $BUFFER_DB_NAME -p $BUFFER_DB_PORT -f "$TMP_DIR"/alyxfull.sql
python $ALYX_DIR/alyx/manage.py makemigrations
python $ALYX_DIR/alyx/manage.py migrate --database public

# Prune local public database
echo "... pruning local public database"
python $ALYX_DIR/alyx/manage.py shell < 01a_prune_public_db.py
# Export the public db to sql
echo "... creating sql dump of local public database"
/usr/bin/pg_dump -cOx -U $BUFFER_DB_USER -h $BUFFER_DB_HOST -d $BUFFER_DB_NAME -p $BUFFER_DB_PORT -f "$TMP_DIR"/openalyx.sql
gzip -f -k "$TMP_DIR"/openalyx.sql
## Copy sql as backup to mbox
echo "... copying sql dump to mbox as backup"
scp "$TMP_DIR"/openalyx.sql.gz "${SSH_STR[*]}":/backups/openalyx-backups/$(date +%Y-%m-%d)_openalyx.sql.gz
echo "$(date '+%Y-%m-%d %H:%M:%S') Finished creating local version of public database"

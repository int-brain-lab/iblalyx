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
# * Adjust the variables below according to your setup (any changes here should also be made in 02_upload_public_db.sh)

# Variables
TMP_DIR="$HOME"/openalyx_wd  # This will be recreated and later destroyed, make sure you have write access
ALYX_DIR=/var/www/alyx-main/alyx # The alyx installation you are using with public db set up
SSH_STR=(mbox)  # ssh/scp alias to connect to the mbox EC2. If you don't have an alias set up you can pass your
                # the ssh/scp options as an array such as (-i ~/.ssh/my_key.pem ubuntu@mbox.internationalbrainlab.org)

echo "$(date '+%Y-%m-%d %H:%M:%S') Beginning to create local version of public database"
# Create a working directory
echo "... creating working directory $TMP_DIR"
mkdir -p "$TMP_DIR"
# Copying and unzipping the most recent alyx backup into it, SSH_STR needs to be setup correctly above
echo "... copying latest backup of production database"
scp "${SSH_STR[@]}":/backups/alyx-backups/$(date +%Y-%m-%d)/alyx_full.sql.gz "$TMP_DIR"/alyxfull.sql.gz
gunzip -f -c "$TMP_DIR"/alyxfull.sql.gz > "$TMP_DIR"/alyxfull.sql

# Source alyx env
source $ALYX_DIR/alyxvenv/bin/activate
# Destroy local public database and load production database into it
echo "... destroying local public database"
python $ALYX_DIR/alyx/manage.py reset_db -D public --noinput
echo "... rebuilding local public database from production"
psql -q -U labdbuser -h localhost -d public -f "$TMP_DIR"/alyxfull.sql
# Prune local public database
echo "... pruning local public database"
python $ALYX_DIR/alyx/manage.py shell < 01a_prune_public_db.py
# Export the public db to sql
echo "... creating sql dump of local public database"
/usr/bin/pg_dump -cOx -U labdbuser -h localhost -d public -f "$TMP_DIR"/openalyx.sql
gzip -f -k "$TMP_DIR"/openalyx.sql
## Copy sql as backup to mbox
echo "... copying sql dump to mbox as backup"
scp "$TMP_DIR"/openalyx.sql.gz "${SSH_STR[@]}":/backups/openalyx-backups/$(date +%Y-%m-%d)_openalyx.sql.gz
echo "$(date '+%Y-%m-%d %H:%M:%S') Finished creating local version of public database"

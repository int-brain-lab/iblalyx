#!/bin/bash
set -e
######################################## BEFORE ########################################
# Make sure that alyx repo on SDSC is on the same commit as repos serving the databases
# Make sure all migrations are applied
# Set Apache on the openalyx instance to Maintenance
########################################################################################

OPENALYX_INSTANCE=ec2-35-177-177-13.eu-west-2.compute.amazonaws.com

TMP_DIR=/home/datauser/temp/openalyx_wd
ALYX_DIR=/home/datauser/Documents/github/alyx

TODAYS_SQL=/mnt/ibl/json/$(date +%Y-%m-%d)_alyxfull.sql.gz
YESTERDAYS_SQL=/mnt/ibl/json/$(date --date='yesterday' '+%Y-%m-%d')_alyxfull.sql.gz

echo "Recreating public database $(date '+%Y-%m-%d')"
# Create a working directory
mkdir -p "$TMP_DIR"
# Unzip the most recent alyx backup into it
if test -f "$TODAYS_SQL"
then
    echo "Unpacking "$TODAYS_SQL" to "$TMP_DIR"/alyxfull.sql"
    gunzip -c "$TODAYS_SQL" > "$TMP_DIR"/alyxfull.sql
else
    echo "Unpacking "$YESTERDAYS_SQL" to "$TMP_DIR"/alyxfull.sql"
    gunzip -c "$YESTERDAYS_SQL" > "$TMP_DIR"/alyxfull.sql
fi


# Source alyx env
source $ALYX_DIR/alyxvenv/bin/activate
# Reset the public database (THIS WILL DESTROY OPENALYX!)
python $ALYX_DIR/alyx/manage.py reset_db -D public --noinput ## never change this part: -D public !!!!
# Load the production alyx sql dump to openalyx
psql -h "$OPENALYX_INSTANCE" -U ibl_dev -d public -f "$TMP_DIR"/alyxfull.sql
# Prune anonymize and create symlinks
python $ALYX_DIR/alyx/manage.py shell < openalyx_pruning.py
# Sync to AWS public bucket
aws s3 sync "/mnt/ibl/public" s3://ibl-brain-wide-map-public/data --exclude "*.zip" --exclude ".*" --profile miles --follow-symlinks --delete
# Remove tmp directory
rm -rf "$TMP_DIR"


#!/bin/bash
set -e

# This script (re-)create the public database through the following steps:
# 1. Destroy the public database
# 2. Replace the public database with the production database
# 3. Prune the public database to contain only the to-be-released information

# Consult the Alyx playbook before running this, as it requires some careful setup. Minimally:
# * Make sure you have the correct database set up as 'public' in the alyx installation you use
# * Make sure all involved databases are on the same branch and commit (production, public, local install)
# * Make sure you have the password for the postgres user on the openalyx RDS instance (Keepass), you need to enter this three times
# * Set the maintenance trigger on the public alyx instance
# * Record the output of this script in a log file and submit these logs to github for the record
# * Adjust the variables below according to your setup

# Variables
OPENALYX_RDS=openlayx.clfrcwlvymbw.eu-west-2.rds.amazonaws.com  # The address of the openalyx database
TMP_DIR="$HOME"/openalyx_wd  # This will be recreated and later destroyed, make sure you have write access
ALYX_DIR=/var/www/alyx-main/alyx # The alyx installation you are using with public db set up
SSH_STR=(mbox)  # ssh/scp alias to connect to the mbox EC2. If you don't have an alias set up you can pass your
                # the ssh/scp options as an array such as (-i ~/.ssh/my_key.pem ubuntu@mbox.internationalbrainlab.org)

echo "$(date '+%Y-%m-%d') Beginning to create public database"
# Create a working directory
echo "... creating working directory $TMP_DIR"
mkdir -p "$TMP_DIR"
# Copying and unzipping the most recent alyx backup into it, SSH_STR needs to be setup correctly above
echo "... copying latest backup of production database"
scp "${SSH_STR[@]}":/backups/alyx-backups/$(date +%Y-%m-%d)/alyx_full.sql.gz "$TMP_DIR"/alyxfull.sql.gz
gunzip -c "$TMP_DIR"/alyxfull.sql.gz > "$TMP_DIR"/alyxfull.sql

# Source alyx env
source $ALYX_DIR/alyxvenv/bin/activate
echo "... destroying the public database"
# Reset the public database
psql -U postgres -h $OPENALYX_RDS -d public -c "drop schema public cascade"
psql -U postgres -h $OPENALYX_RDS -d public -c "create schema public"
# Load the production alyx sql dump to public db
echo "... recreating public database from production"
psql -U postgres -h $OPENALYX_RDS -d public -f "$TMP_DIR"/alyxfull.sql
# Prune public database
echo "... pruning public database"
python $ALYX_DIR/alyx/manage.py shell < 01a_prune_public_db.py
# Remove tmp directory
rm -rf "$TMP_DIR"
echo "Finished creating public database"

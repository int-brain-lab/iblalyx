#!/bin/bash
set -e

# This script destroys the openalyx database and replaces it with an sql dump of the local public database
# created by 01_create_public_db.sh

# Consult the Alyx playbook before running this, as it requires some careful setup. Minimally:
# * Make sure all involved databases are on the same branch and commit (production, public, local install)
# * Make sure you have the password for the postgres user on the openalyx RDS instance (Keepass)
# * Record the output of this script in a log file and submit these logs to github for the record
# * Adjust the variables in the .env file according to your setup (see the .env_template file for the variables)

source .env

# Reset the public database on the RDS instance
echo "$(date '+%Y-%m-%d %H:%M:%S') Beginning to upload database to openalyx RDS"
echo "... destroying the openalyx database"
psql -U $OPENALYX_USER -h $OPENALYX_RDS -d $OPENALYX_NAME -c "drop schema public cascade"
psql -U $OPENALYX_USER -h $OPENALYX_RDS -d $OPENALYX_NAME -c "create schema public"

# Load the production alyx sql dump to public db
echo "... loading pruned database to openalyx"
psql -q -U $OPENALYX_USER -h $OPENALYX_RDS -d $OPENALYX_NAME -f "$TMP_DIR"/openalyx.sql

# Remove tmp directory
rm -rf "$TMP_DIR"
echo "$(date '+%Y-%m-%d %H:%M:%S') Finished uploading database to openalyx RDS"

#!/bin/bash
set -e

# This script destroys the openalyx database and replaces it with an sql dump of the local public database
# created by 01_create_public_db.sh

# Consult the Alyx playbook before running this, as it requires some careful setup. Minimally:
# * Make sure all involved databases are on the same branch and commit (production, public, local install)
# * Make sure you have the password for the postgres user on the openalyx RDS instance (Keepass)
# * Record the output of this script in a log file and submit these logs to github for the record
# * Adjust the variables below according to your setup (copy any changes from 02_upload_public_db.sh), in particular
# make sure the OPENALYX_RDS address is still correct

# Variables
OPENALYX_RDS=openlayx.clfrcwlvymbw.eu-west-2.rds.amazonaws.com  # The address of the openalyx database
TMP_DIR="$HOME"/openalyx_wd  # This will be recreated and later destroyed, make sure you have write access

# Reset the public database on the RDS instance
echo "$(date '+%Y-%m-%d %H:%M:%S') Beginning to upload database to openalyx RDS"
echo "... destroying the openalyx database"
psql -U postgres -h $OPENALYX_RDS -d public -c "drop schema public cascade"
psql -U postgres -h $OPENALYX_RDS -d public -c "create schema public"

# Load the production alyx sql dump to public db
echo "... loading pruned database to openalyx"
psql -q -U postgres -h $OPENALYX_RDS -d public -f "$TMP_DIR"/openalyx.sql

# Remove tmp directory
rm -rf "$TMP_DIR"
echo "$(date '+%Y-%m-%d %H:%M:%S') Finished uploading database to openalyx RDS"

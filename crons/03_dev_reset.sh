#!/bin/bash
set -e
HOST=alyx.clfrcwlvymbw.eu-west-2.rds.amazonaws.com
DATE_STR=$(date +%Y-%m-%d)
backup_dir="/backups/alyx-backups/$DATE_STR"
mkdir -p "$backup_dir"

# Delete the full dev database
psql -q -U ibl_dev -h $HOST -d alyx_dev -c "drop schema public cascade"
psql -q -U ibl_dev -h $HOST -d alyx_dev -c "create schema public"

# unzip the sql
gunzip -k $backup_dir/alyx_full.sql.gz

# Loads the main database into the dev one
psql -h $HOST -U ibl_dev -d alyx_dev -f $backup_dir/alyx_full.sql
rm $backup_dir/alyx_full.sql

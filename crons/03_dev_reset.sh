#!/bin/bash
set -e
HOST=alyx.clfrcwlvymbw.eu-west-2.rds.amazonaws.com
backup_dir="/backups/alyx-backups/$(date +%Y-%m-%d)"
mkdir -p "$backup_dir"

# Delete the full dev database
psql -q -U ibl_dev -h $HOST -d alyx_dev -c "drop schema public cascade"
psql -q -U ibl_dev -h $HOST -d alyx_dev -c "create schema public"
# unzip the sql
gunzip -k $backup_dir/alyx_full.sql.gz
# Loads the main database into the dev one
psql -h $HOST -U ibl_dev -d alyx_dev -f $backup_dir/alyx_full.sql
rm $backup_dir/alyx_full.sql
# Mirror the migrations from alyx-main to alyx-dev: wipe them all out and then copy from alyx-main
rm -r /var/www/alyx-dev/alyx/*/migrations/0*
cp -r /var/www/alyx-main/alyx/misc/migrations/0* /var/www/alyx-dev/alyx/misc/migrations/
cp -r /var/www/alyx-main/alyx/data/migrations/0* /var/www/alyx-dev/alyx/data/migrations/
cp -r /var/www/alyx-main/alyx/actions/migrations/0* /var/www/alyx-dev/alyx/actions/migrations/
cp -r /var/www/alyx-main/alyx/subjects/migrations/0* /var/www/alyx-dev/alyx/subjects/migrations/
# Apply migrations (if any)
cd /var/www/alyx-dev/alyx
source ../venv/bin/activate
./manage.py makemigrations
./manage.py migrate

python /var/www/alyx-main/scripts/deployment_examples/99_purge_duplicate_backups.py

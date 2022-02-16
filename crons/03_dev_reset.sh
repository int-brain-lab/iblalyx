#!/bin/bash
set -e  # exit immediately if a command exits with a non-zero status

# DB vars
HOST=alyx-dev.clfrcwlvymbw.eu-west-2.rds.amazonaws.com
USERNAME=alyx-dev
DBNAME=alyx-dev

# Filesystem vars
BACKUP_DIR="/backups/alyx-backups/$(date +%Y-%m-%d)"
BACKUP_FILE_NAME=alyx_full_test.tar
COMPRESSED_FILE_NAME="$BACKUP_FILE_NAME.gz"

# Create the backup directory and set as working directory in case it does not already exist (this would be a problem)
mkdir -p "$BACKUP_DIR"; cd "$BACKUP_DIR"

# uncompress the archive
tar -xzvf $COMPRESSED_FILE_NAME
# -z - Use gzip compression
# -v - Provide verbose output
# -f - Archive file name
# -x - Extract from a compressed file

# Drop/delete the public schema
psql --quiet \
     --host=$HOST \
     --username=$USERNAME \
     --dbname=$DBNAME \
     --command="drop schema public cascade"

# Recreate the public schema
psql --quiet \
     --host=$HOST \
     --username=$USERNAME \
     --dbname=$DBNAME \
     --command="create schema public"

# Loads the main database backup into the dev environment
pg_restore --host=$HOST \
           --username=$USERNAME \
           --dbname=$DBNAME \
           --no-owner \
           --no-acl \
           --schema=public \
           $BACKUP_FILE_NAME

# Remove the non-compressed file now that we have loaded it into the dev db
rm $BACKUP_FILE_NAME

# Mirror the migrations from alyx-main to alyx-dev: wipe them all out and then copy from alyx-main
rm -r /var/www/alyx-dev/alyx/*/migrations/0*
cp -r /var/www/alyx-main/alyx/misc/migrations/0* /var/www/alyx-dev/alyx/misc/migrations/
cp -r /var/www/alyx-main/alyx/data/migrations/0* /var/www/alyx-dev/alyx/data/migrations/
cp -r /var/www/alyx-main/alyx/actions/migrations/0* /var/www/alyx-dev/alyx/actions/migrations/
cp -r /var/www/alyx-main/alyx/subjects/migrations/0* /var/www/alyx-dev/alyx/subjects/migrations/

# Apply django migrations (if any)
cd /var/www/alyx-dev/alyx
source ../venv/bin/activate
./manage.py makemigrations
./manage.py migrate

python /var/www/alyx-main/scripts/deployment_examples/99_purge_duplicate_backups.py

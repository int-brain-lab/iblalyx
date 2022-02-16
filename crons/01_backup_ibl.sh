#!/bin/bash
set -e  # exit immediately if a command exits with a non-zero status

# DB vars
HOST=alyx.clfrcwlvymbw.eu-west-2.rds.amazonaws.com
USERNAME=ibl_dev
DBNAME=alyx

# Filesystem vars
BACKUP_DIR="/backups/alyx-backups/$(date +%Y-%m-%d)"
BACKUP_FILE_NAME=alyx_full.tar
COMPRESSED_FILE_NAME="$BACKUP_FILE_NAME.gz"

# Create the backup directory and set as working directory
mkdir -p "$BACKUP_DIR"; cd "$BACKUP_DIR"

# SQL dump to archive file
pg_dump --host=$HOST \
        --username=$USERNAME \
        --dbname=$DBNAME \
        --no-acl \
        --format=tar \
        --file=$BACKUP_FILE_NAME

# Compress tar for rsync to flatiron, file is used daily for resetting the dev db with the prod db
tar -czvf $COMPRESSED_FILE_NAME $BACKUP_FILE_NAME
# -c - Create a new archive
# -z - Use gzip compression
# -v - Provide verbose output
# -f - Archive file name

# Transfer compressed archive to flatiron with rsync
rsync -av --progress -e "ssh -i /home/ubuntu/.ssh/sdsc_alyx.pem -p 62022" \
  "$COMPRESSED_FILE_NAME" alyx@ibl.flatironinstitute.org:/mnt/ibl/json/$(date +%Y-%m-%d)_alyxfull.tar.gz

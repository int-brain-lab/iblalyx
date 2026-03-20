#!/bin/bash
# Source env variables and alyx env
export $(grep -v '^#' .env | xargs)

set -e
BACKUP_DATE=$(date -d 'today' +%Y-%m-%d)

# Create a working directory
echo "... creating working directory $TMP_DIR"
mkdir -p "$TMP_DIR"
# Copying and unzipping the most recent alyx backup into it, SSH_STR needs to be setup correctly above
echo "... copying latest backup of production database"
wget -O "$TMP_DIR/alyxfull.sql.gz" --user="$WGET_USER" --password="$WGET_PASSWORD" "https://ibl.flatironinstitute.org/json/${BACKUP_DATE}_alyxfull.sql.gz"
gunzip -f "$TMP_DIR/alyxfull.sql.gz"

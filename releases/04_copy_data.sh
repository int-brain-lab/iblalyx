#!/bin/bash
set -e
# This script needs to be run on the SDSC server as datauser, in a tmux shell
# It is run AFTER the public database is reset and recreated.
# Make sure the alyx installation on SDSC is connected to the openalyx RDS as database 'public'

ALYX_DIR="$HOME"/Documents/PYTHON/alyx/alyx

# Sync to AWS public bucket
echo "$(date '+%Y-%m-%d %H:%M:%S') Syncing to public S3 bucket data/"
aws s3 sync "/mnt/ibl/public" s3://ibl-brain-wide-map-public/data --exclude "aggregates/*" --exclude "*.zip" --exclude ".*" --profile ibladmin --follow-symlinks --delete --no-progress --only-show-errors
echo "$(date '+%Y-%m-%d %H:%M:%S') Syncing to public S3 bucket aggregates/"
aws s3 sync "/mnt/ibl/public/aggregates" s3://ibl-brain-wide-map-public/aggregates --exclude ".*" --exclude "logs/*" --profile ibladmin --follow-symlinks --delete --no-progress --only-show-errors
echo "$(date '+%Y-%m-%d %H:%M:%S') Finished"

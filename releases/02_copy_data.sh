#!/bin/bash
set -e
# This script needs to be run on the SDSC server as datauser, in a tmux shell
# It is run AFTER the public database is reset and recreated.
# Make sure the alyx installation on SDSC is connected to the openalyx RDS as database 'public'
# After this script, remove the maintenance trigger for openalyx

OPENALYX_RDS=openlayx.clfrcwlvymbw.eu-west-2.rds.amazonaws.com
ALYX_DIR="$HOME"/Documents/github/alyx/alyx

# Activate alyx env
source $ALYX_DIR/alyxvenv/bin/activate
# Create all missing symlinks in /mnt/ibl/public for the public database
python $ALYX_DIR/alyx/manage.py shell < openalyx_symlinks.py
# Sync to AWS public bucket
echo "Syncing to public S3 bucket"
aws s3 sync "/mnt/ibl/public" s3://ibl-brain-wide-map-public/data --exclude "*.zip" --exclude ".*" --profile ibladmin --follow-symlinks --delete


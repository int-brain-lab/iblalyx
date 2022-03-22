#!/bin/bash
set -e
######################################## BEFORE ######################
# Make sure that all alyx repos used are on the same branch and commit
# Make sure all migrations are applied
# Set Apache on the openalyx instance to Maintenance
#######################################################################

OPENALYX_INSTANCE=openalyx.internationalbrainlab.org
TMP_DIR=/home/julia/openalyx_wd
ALYX_DIR=/var/www/alyx-main/alyx

echo "Recreating public database $(date '+%Y-%m-%d')"
# Create a working directory
mkdir -p "$TMP_DIR"
# Copying and unzipping the most recent alyx backup into it
# Assumes that you have an ssh to the production alyx instance set up as in 'ssh alyx'
scp alyx:/backups/alyx-backups/$(date +%Y-%m-%d)/alyx_full.sql.gz "$TMP_DIR"/alyxfull.sql.gz
gunzip -c "$TMP_DIR"/alyxfull.sql.gz > "$TMP_DIR"/alyxfull.sql

# Source alyx env
source $ALYX_DIR/alyxvenv/bin/activate
# Reset the local installation of the public database
python $ALYX_DIR/alyx/manage.py reset_db -D public --noinput
# Load the production alyx sql dump to the local public db
psql -h localhost -U labdbuser -d public -f "$TMP_DIR"/alyxfull.sql
python $ALYX_DIR/alyx/manage.py migrate --database=public
# Prune public database
python $ALYX_DIR/alyx/manage.py shell < openalyx_pruning.py
# Dump the pruned database
echo "Dumping pruned database locally"
/usr/bin/pg_dump -cOx -U labdbuser -h localhost -d public -f "$TMP_DIR"/alyx_public.sql
# Reset openalyx database and load pruned database there
echo "Resetting openalyx"
psql -q -U ibl_dev -h $OPENALYX_INSTANCE -d ibl -c "drop schema public cascade"
psql -q -U ibl_dev -h $OPENALYX_INSTANCE -d ibl -c "create schema public"
psql -h $OPENALYX_INSTANCE -U ibl_dev -d ibl -f "$TMP_DIR"/alyx_public.sql
python $ALYX_DIR/alyx/manage.py migrate --database=openalyx
# Remove tmp directory
rm -rf "$TMP_DIR"


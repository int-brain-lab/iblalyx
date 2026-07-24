#!/bin/bash
set -e

# This script prunes the buffer database (already populated by 01a_download_database.sh) down
# to only the to-be-released information. 02_upload_public_db.sh dumps the result: once to back
# up to flatiron, once to load onto the real openalyx RDS.

# Consult the Alyx playbook before running this, as it requires some careful setup. Minimally:
# * Make sure production, buffer and openalyx are all on the same alyx branch/commit
# * Record the output of this script in a log file (and submit these logs to github for the record later)

# Source env variables
export $(grep -v '^#' ../docker/.env | xargs)

echo "$(date '+%Y-%m-%d %H:%M:%S') Beginning to prune buffer database"

echo "... pruning buffer database"
docker exec -i ibl_alyx_apache bash -c "python /home/iblalyx/releases/01b_prune_public_db.py"

echo "$(date '+%Y-%m-%d %H:%M:%S') Finished pruning buffer database"

#!/bin/bash
set -euo pipefail
HOST=alyx.clfrcwlvymbw.eu-west-2.rds.amazonaws.com

## Full SQL dump.
pg_dump -h $HOST -p 5432 -U ibl_dev -d alyx | gzip -1 | ssh -i /home/ubuntu/.ssh/sdsc_alyx.pem -p 62022 alyx@ibl-ssh.flatironinstitute.org 'set -e; target="/mnt/ibl/json/$(date +%F)_alyxfull.sql.gz"; part="${target}.part"; cat > "$part" && mv "$part" "$target"'

# Trim down the reversions to the last 15 days
cd /home/Documents/PYTHON/iblalyx/crons/mbox/import
docker exec ibl_alyx_apache python /var/www/alyx/alyx/manage.py deleterevisions --days=15

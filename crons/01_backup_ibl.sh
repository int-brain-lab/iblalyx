#!/bin/bash
set -e
HOST=alyx.clfrcwlvymbw.eu-west-2.rds.amazonaws.com
backup_dir="/backups/alyx-backups/$(date +%Y-%m-%d)"
mkdir -p "$backup_dir"

# Full SQL dump.
/usr/bin/pg_dump -cOx -U ibl_dev -h $HOST -d alyx -f "$backup_dir/alyx_full.sql"
gzip -f "$backup_dir/alyx_full.sql"
rsync -av --progress -e "ssh -i /home/ubuntu/.ssh/sdsc_alyx.pem -p 62022" "$backup_dir/alyx_full.sql.gz" alyx@ibl.flatironinstitute.org:/mnt/ibl/json/$(date +%Y-%m-%d)_alyxfull.sql.gz

# clean up the backups on AWS instance
source /var/www/alyx-main/venv/bin/activate
python /var/www/alyx-main/scripts/deployment_examples/99_purge_duplicate_backups.py

# Trim down the reversions to the last 15 days
python /var/www/alyx-main/alyx/manage.py deleterevisions --days=15

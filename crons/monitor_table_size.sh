#!/bin/bash
set -e
HOST=alyx.clfrcwlvymbw.eu-west-2.rds.amazonaws.com
SQL_PATH=/home/ubuntu/iblalyx/sql
backup_dir=/backups/alyx-backups/table_sizes
mkdir -p $backup_dir

# Full SQL dump.
/usr/bin/psql -U ibl_dev -h $HOST -d alyx -f $SQL_PATH/table_sizes.sql > $backup_dir/$(date +%Y-%m-%d)_pg_stats.txt

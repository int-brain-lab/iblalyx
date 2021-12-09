ALYX_DIR="/var/www/alyx-main"
source /home/ubuntu/alyxvenv/bin/activate
cd $ALYX_DIR/alyx

backup_dir="$ALYX_DIR/alyx-backups/$(date +%Y-%m-%d)"
mkdir -p "$backup_dir"

# Full SQL dump.
echo "SQL dump..."
/usr/bin/pg_dump -cOx -U ibl_dev -h localhost -d ibl -f "$backup_dir/alyx_full.sql"
gzip -f "$backup_dir/alyx_full.sql"
echo "SQL dump complete"

# Full django JSON dump.
echo "JSON dump"
./manage.py dumpdata -e contenttypes -e auth.permission -e reversion.version -e reversion.revision -e admin.logentry --indent 1 > "$backup_dir/alyx_full.json"
gzip -f "$backup_dir/alyx_full.json"
echo "JSON dump complete"
# Human-readable TSV backup and Google Spreadsheets backup.
#/var/www/alyx/alyx/bin/python /var/www/alyx/alyx/manage.py backup /var/www/alyx/alyx-backups/


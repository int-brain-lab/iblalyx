# date from 1 week ago
old_date=$(date +'%Y-%m-%d' -d 'now - 1 week')
# move the old backup to the larger drive
mv /var/www/alyx-main/alyx-backups/$old_date /data/alyx-backups/

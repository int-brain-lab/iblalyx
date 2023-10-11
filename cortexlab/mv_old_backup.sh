#!/bin/bash

# date from 1 week ago
old_date=$(date +'%Y-%m-%d' -d 'now - 1 week')

# move the old backup to the larger drive
src="/var/www/alyx-main/alyx-backups/$old_date"
dst="/data/alyx-backups/"
mv $src $dst

# delete an old backup
old_old_date=$(date +'%Y-%m-%d' -d 'now - 3 months')
old_backup="/data/alyx-backups/$old_old_date/"
if [ -d $old_backup ]; then
  echo "removing $old_backup"
  rm -rf $old_backup
else
  echo "no backup for $old_old_date to remove..."
fi

# check if any folders still unchanged
if [ -d $src ] || [ -d $old_backup ]; then
  # send a message about backup failure
  recipient=$"devs@internationalbrainlab.org"
  echo "either $src or $old_backup unchanged; sending email report to $recipient"
  local_usage=$(df -hP /dev/root | awk '{print $5}' |tail -1|sed 's/%$//g')
  dst_usage=$(df -hP /data | awk '{print $5}' |tail -1|sed 's/%$//g')
  body="Failed to move old backups from $src to $dst and remove $old_backup. "
  body+="Local disk usage is at $local_usage%; destination usage is at $dst_usage%."
  echo $body | mail -s "Alyx cortexlab backup failure" $recipient
fi


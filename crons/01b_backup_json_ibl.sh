backup_dir="/backups/alyx-backups/$(date +%Y-%m-%d)"

# Full django JSON dump, used by datajoint
mkdir -p "$backup_dir"
# Full django JSON dump.
source /var/www/alyx-main/venv/bin/activate
python /var/www/alyx-main/alyx/manage.py dumpdata \
    -e contenttypes -e auth.permission \
    -e reversion.version -e reversion.revision -e admin.logentry \
    -e actions.ephyssession \
    -e actions.notification \
    -e actions.notificationrule \
    -e actions.virusinjection \
    -e data.download \
    -e experiments.brainregion \
    -e jobs.task \
    -e misc.note \
    -e subjects.subjectrequest \
    --indent 1 -o "alyx_full.json"

gzip -f "$backup_dir/alyx_full.json"

# Send the files to the FlatIron server
rsync -av --progress -e "ssh -i /home/ubuntu/.ssh/sdsc_alyx.pem -p 62022" "$backup_dir/alyx_full.json.gz" alyx@ibl.flatironinstitute.org:/mnt/ibl/json/alyxfull.json.gz

# clean up the backups on AWS instance
python /var/www/alyx-main/scripts/deployment_examples/99_purge_duplicate_backups.py

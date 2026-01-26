cd /var/www/alyx/alyx
# Delete all tasks older than 2 years
before_date=$(date -d "2 years ago" +"%Y-%m-%d")
python ./manage.py tasks cleanup --before $before_date
# Delete complete tasks older than 2 months
before_date=$(date -d "2 months ago" +"%Y-%m-%d")
python ./manage.py tasks cleanup --before $before_date --status complete
# Delete tasks associated with signed-off sessions
python ./manage.py tasks cleanup --before $before_date --signed-off

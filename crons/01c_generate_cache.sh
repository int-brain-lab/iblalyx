# NB This is run on a cron job
# Alyx cache tables
source /var/www/alyx-main/venv/bin/activate
python /var/www/alyx-main/alyx/manage.py one_cache -v 2 --compress

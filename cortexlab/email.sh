source ~/alyxvenv/bin/activate
cd /var/www/alyx-main/alyx
./manage.py check_water_admin
./manage.py send_pending_notifications

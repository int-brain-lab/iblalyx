#!/bin/bash
set -e
source ~/alyxvenv/bin/activate
/var/www/alyx-main/alyx/manage.py shell < ~/iblalyx/crons/monitor_dlc.py >> ~/dlc_remaining.txt

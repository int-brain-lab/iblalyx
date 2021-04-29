#!/bin/bash
set -e
source ~/alyxvenv/bin/activate
/var/www/alyx-main/alyx/manage.py shell < ~/qc_table.py >> ˜/dlc_remaining.txt

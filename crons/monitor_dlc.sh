#!/bin/bash
set -e
source ~/alyxvenv/bin/activate
/var/www/alyx-main/alyx/manage.py ibl monitor_dlc >> ~/dlc_remaining.txt

#!/bin/bash
set -e
source ~/alyxvenv/bin/activate
/var/www/alyx-main/alyx/manage.py ibl histology_assign_update

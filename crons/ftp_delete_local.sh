#!/bin/bash
set -e
source ~/alyxvenv/bin/activate
/var/www/alyx-main/alyx/manage.py shell < ~/iblalyx/crons/ftp_delete_local.py
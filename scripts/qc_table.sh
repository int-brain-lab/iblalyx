#!/bin/bash
set -e
source ~/alyxvenv/bin/activate
/var/www/alyx-main/alyx/manage.py shell < ~/qc_table.py

rsync -av --progress -e "ssh -i /home/ubuntu/.ssh/sdsc_alyx.pem -p 62022" "/home/ubuntu/qc_table.pqt" datauser@ibl.flatironinstitute.org:/mnt/ibl/tables/qc_table.pqt

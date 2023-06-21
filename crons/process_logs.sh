#!/bin/bash
source /var/www/alyx-main/venv/bin/activate
python ~/iblalyx/scripts/s3_logs/process_logs.py $1 $2

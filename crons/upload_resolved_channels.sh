#!/bin/bash
# A cron script to check for insertions that have been resolved and create and register the relevant channels datasets
# Run on SDSC via a cronjob on mbox every day
ssh -i /home/ubuntu/.ssh/sdsc_alyx.pem -p 62022 datauser@ibl.flatironinstitute.org "cd ~/Documents/PYTHON/alyx/alyx; cat ~/Documents/PYTHON/iblalyx/scripts/upload_resolved_channels.py | ~/Documents/PYTHON/alyx/alyx/alyxvenv/bin/python manage.py shell > ~/Documents/upload_channels.log 2>&1"

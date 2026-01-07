#!/bin/bash
# A cron script to sync datasets from AWS patcher to flatiron
# Run on SDSC via a cronjob on mbox every day
ssh -i /home/ubuntu/.ssh/sdsc_alyx.pem -p 62022 datauser@ibl-ssh.flatironinstitute.org "cd ~/Documents/PYTHON/alyx/alyx; source ../alyxvenv/bin/activate; ./manage.py sync_patcher sync > ~/Documents/sync_patcher.log 2>&1"

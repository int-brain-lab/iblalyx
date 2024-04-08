#!/bin/bash
# A cron script to check for insertions that have been resolved and create and register the relevant channels datasets
# Run on SDSC via a cronjob on mbox every day
ssh -i /home/ubuntu/.ssh/sdsc_alyx.pem -p 62022 datauser@ibl.flatironinstitute.org "cd ~/Documents/github/alyx; source alyx/alyxvenv/bin/activate; python alyx/manage.py shell < ~/Documents/PYTHON/iblscripts/analysis_crons/subject_trials_aggregates.py > /mnt/ibl/aggregates/logs/subjectTrials.log 2>&1"

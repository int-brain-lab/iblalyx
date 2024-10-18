#!/bin/bash
# A cron script to check for insertions that have been resolved and create and register the relevant channels datasets
# Run on SDSC via a cronjob on mbox every day
ssh -i /home/ubuntu/.ssh/sdsc_alyx.pem -p 62022 datauser@ibl.flatironinstitute.org "cd ~/Documents/PYTHON/alyx/alyx; source ../alyxvenv/bin/activate; python ~/Documents/PYTHON/iblalyx/scripts/aggregate_all_subjects_trials.py --only-new-subjects > /mnt/ibl/aggregates/logs/subjectTrials.log 2>&1"

#!/bin/bash
# A cron script to sync recently added/modified datasets to AWS.  This script is run from FlatIron.
# $1 - Sync datasets modified within this many hours
ssh -i /home/ubuntu/.ssh/sdsc_alyx.pem -p 62022 datauser@ibl.flatironinstitute.org "cd ~/Documents/github/alyx; source alyx/alyxvenv/bin/activate; python alyx/manage.py update_aws --hours $1 -v 2 > ~/Documents/aws_sync.log 2>&1"

#!/bin/bash
# /bin/bash ~/iblalyx/crons/sdsc/sync_aws.sh > ~/sdsc_aws_sync.log 2>&1
# WARNING this currently fails as it's syncing the entire contents of /mnt/ibl, not only the lab

# Sync a given lab to the private S3 bucket
LAB="steinmetzlab"

# A function to display number of seconds as hh:mm:ss
format_time() {
  ((h=${1}/3600))
  ((m=(${1}%3600)/60))
  ((s=${1}%60))
  printf "%02d:%02d:%02d\n" $h $m $s
 }

echo "starting sync of $LAB"
ssh -i /home/ubuntu/.ssh/sdsc_alyx.pem -p 62022 datauser@ibl.flatironinstitute.org 'aws s3 sync "/mnt/ibl/$LAB" s3://ibl-brain-wide-map-private/data --exclude ".*" --profile miles --delete'

echo "Sync completed in $(format_time $SECONDS)"
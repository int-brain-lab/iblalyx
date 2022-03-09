#!/bin/bash
# /bin/bash ~/iblalyx/crons/sdsc/sync_aws.sh angelakilab > ~/sdsc_aws_sync.log 2>&1
# Dry run of Steinmetz lab with 860 sessions took 7 min 50 sec

# Sync a given lab to the private S3 bucket; lab name given by first arg

# A function to display number of seconds as hh:mm:ss
format_time() {
  ((h=${1}/3600))
  ((m=(${1}%3600)/60))
  ((s=${1}%60))
  printf "%02d:%02d:%02d\n" $h $m $s
 }

echo "starting sync of $1"
aws s3 sync "/mnt/ibl/$1" "s3://ibl-brain-wide-map-private/data/$1" --exclude ".*" --profile ibladmin --delete

echo "Sync completed in $(format_time $SECONDS)"

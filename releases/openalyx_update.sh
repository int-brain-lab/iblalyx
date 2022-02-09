#!/bin/bash

######################################## BEFORE ########################################
# Make sure that alyx repo on SDSC is on the same commit as repos serving the databases
# Make sure all migrations are applied
# Set Apache on the openalyx instance to Maintenance
########################################################################################

WORKING_DIR=/home/datauser/openalyx_wd
ALYX_DIR=/home/datauser/Documents/github/alyx
AWS_INFO_FILE=/home/datauser/Documents/aws_public_info.json

TODAYS_SQL=/mnt/ibl/json/$(date +%Y-%m-%d)_alyxfull.sql.gz
YESTERDAYS_SQL=/mnt/ibl/json/$(date --date='yesterday' '+%Y-%m-%d')_alyxfull.sql.gz

# Create a working directory
mkdir -p $WORKING_DIR
# Unzip the most recent alyx backup into it
if test -f $TODAYS_SQL
then
    echo "Unpacking $TODAYS_SQL to $WORKING_DIR/alyxfull.sql"
    gunzip -c $TODAYS_SQL > $WORKING_DIR/alyxfull.sql
else
    echo "Unpacking $YESTERDAYS_SQL to $WORKING_DIR/alyxfull.sql"
    gunzip -c $YESTERDAYS_SQL > $WORKING_DIR/alyxfull.sql
fi

# Source alyx env
source $ALYX_DIR/alyx/alyxvenv/bin/activate
# Reset the public database (THIS WILL DESTROY OPENALYX!)
python $ALYX_DIR/alyx/manage.py reset_db -D public  # possibly use --no-input option, but requiring confirmation for now
# Load the production alyx sql dump to openalyx
psql -h ec2-35-177-177-13.eu-west-2.compute.amazonaws.com -U ibl_dev -d public -f $WORKING_DIR/alyxfull.sql
# Prune and anonymize
python openalyx_pruning.py $AWS_INFO_FILE
########################################
# Create symlinks for public data
##########################################
# Sync to AWS
##########################################

###### AFTER #######
# Unset maintenance
####################



#!/bin/bash
# ./06_ucl_import.sh 2022-08-19
ALYX_PATH="/var/www/alyx-main/"
DATE="${1:-$(date +%Y-%m-%d)}"

set -e
echo "Downloading the cortexlab database backup"
cd $ALYX_PATH
scp -i /home/ubuntu/.ssh/sdsc_alyx.pem ubuntu@alyx.cortexlab.net:/var/www/alyx-main/alyx-backups/$DATE/alyx_full.sql.gz ./scripts/sync_ucl/cortexlab.sql.gz
gunzip -f ./scripts/sync_ucl/cortexlab.sql.gz

echo "Reinitialize the cortexlab database"
psql -q -U ibl_dev -h localhost -d cortexlab -c "drop schema public cascade"
psql -q -U ibl_dev -h localhost -d cortexlab -c "create schema public"
psql -h localhost -U ibl_dev -d cortexlab -f ./scripts/sync_ucl/cortexlab.sql
rm ./scripts/sync_ucl/cortexlab.sql

cd alyx
source ../venv/bin/activate
./manage.py migrate --database cortexlab
echo "Cascade deleting all non-IBL subjects"
./manage.py shell < ../scripts/sync_ucl/prune_cortexlab.py
echo "Load pruned cortexlab data into ibl"
./manage.py loaddata ../scripts/sync_ucl/cortexlab_pruned.json 

# set the cortex lab json field health report and set reverse fk probeinsertion --> dataset
./manage.py shell < ../scripts/sync_ucl/ucl_post_json_import.py

# sync the uploaded folder on cortexlab alyx to alyx-uploaded s3 bucket
ssh -i ~/.ssh/sdsc_alyx.pem ubuntu@ec2-18-132-246-163.eu-west-2.compute.amazonaws.com "aws s3 sync /var/www/alyx-main/uploaded s3://alyx-uploaded/uploaded"

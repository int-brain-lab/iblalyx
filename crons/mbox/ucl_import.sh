#!/bin/bash
# Create the database if it doesn't exist
docker exec -i ucl_alyx_postgres psql -q -U ibl_dev -d postgres -c "CREATE DATABASE cortexlab;" 2>/dev/null || true
# Drop and recreate schema
docker exec -i ucl_alyx_postgres psql -q -U ibl_dev -d cortexlab -c "drop schema public cascade; create schema public"
# Import the data from the remote alyx database into the local cortexlab database
pg_dump -h alyx.cpd8pxc0pbj7.eu-west-2.rds.amazonaws.com -U alyx_admin -d alyx --no-owner --no-acl \
| docker exec -i ucl_alyx_postgres psql -U ibl_dev -d cortexlab
# Ensure database migrated
docker exec -i ibl_alyx_apache bash -c "./manage.py migrate --database cortexlab"
# Prune imported data and import into IBL database
echo "Cascade deleting all non-IBL subjects"
docker exec -i ibl_alyx_apache bash -c "python /home/iblalyx/scripts/prune_cortexlab.py"

# sync the uploaded folder on cortexlab alyx to alyx-uploaded s3 bucket
aws s3 sync s3://alyx-cortexlab/uploaded s3://alyx-uploaded/uploaded

#!/bin/bash
set -e
# Delete the full dev database
whoami
psql -q -U ibl_user -h localhost -d ibl_test -c "drop schema public cascade"
psql -q -U ibl_user -h localhost -d ibl_test -c "create schema public"
# Loads the main database into the dev one
psql -h localhost -U ibl_user -d ibl_test -f /var/www/alyx-dev/data/alyx_full.sql
# activate environment
source /home/ubuntu/alyxvenv/bin/activate
cd /var/www/alyx-dev/alyx
# django migrations
./manage.py makemigrations
./manage.py migrate
../scripts/load-init-fixtures.sh
# permissions
./manage.py set_db_permissions
./manage.py set_user_permissions


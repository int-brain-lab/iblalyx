#!/bin/bash
#./01_write_db_to_cache.sh /home/ubuntu/current_database.sql
set -e
docker exec -e PGPASSWORD=postgres alyx_postgres sh -c '/usr/bin/pg_dump -cOx -h $POSTGRES_HOST -U $POSTGRES_USER -d $POSTGRES_DB -f /home/alyx_test.sql'
docker cp alyx_postgres:/home/alyx_test.sql $1

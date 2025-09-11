#!/bin/bash
docker stop postgres-alyx-client
docker rm postgres-alyx-client
set -e
docker run -d -e POSTGRES_PASSWORD=postgres -e PGPASSFILE=/.pgpass -v /home/ubuntu/tmp:/backups --name postgres-alyx-client postgres:17

cat .pgpass | docker exec -i postgres-alyx-client sh -c 'cat >.pgpass'
docker exec -i postgres-alyx-client sh -c 'chmod 600 .pgpass && chown postgres:postgres .pgpass'

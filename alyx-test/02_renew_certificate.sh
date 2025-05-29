#!/bin/bash
set -e
docker exec -it alyx_apache certbot --apache --noninteractive --agree-tos --email admin@internationalbrainlab.org -d test.alyx.internationalbrainlab.org

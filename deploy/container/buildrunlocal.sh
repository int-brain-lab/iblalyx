#!/bin/bash
set -e
docker buildx build . --platform linux/amd64 --tag internationalbrainlab/alyx_base:latest -f ./Dockerfile_base
docker buildx build . --platform linux/amd64 --tag internationalbrainlab/alyx:latest -f ./Dockerfile --no-cache

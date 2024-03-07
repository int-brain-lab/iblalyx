    #!/bin/bash
    set -e
    docker buildx build ./ \
        --platform linux/amd64 \
        --tag public.ecr.aws/p4h6o9n8/alyx:latest \
        --build-arg DATETIME=$(date +%Y-%m-%d-%H:%M:%S)
    docker run  \
        --env-file environment.env \
        --platform linux/amd64 \
        -p 8000:8000 \
        public.ecr.aws/p4h6o9n8/alyx:latest

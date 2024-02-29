    docker buildx build ./ \
        --platform linux/amd64 \
        --tag 537761737250.dkr.ecr.eu-west-2.amazonaws.com/iblalyx:latest \
        --build-arg DATETIME=$(date +%Y-%m-%d-%H:%M:%S)
    docker run  \
        --env-file environment.env \
        --platform linux/amd64 \
        -p 8000:8000 \
        537761737250.dkr.ecr.eu-west-2.amazonaws.com/iblalyx:latest

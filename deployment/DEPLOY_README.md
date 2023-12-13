# Alyx webapp deployment guide

We are going to deploy the Alyx webapp on AWS using Docker.

In our case, we will deploy Alyx on the Coud using the following services
- the postgres database is hosted on AWS RDS
- the static files and media files are hosted on AWS S3
- the container is built on AWS ECR, as a public repository
- the container is deployed on AWS App Runner

## Building the Docker container

### Description of the Dockerfile
The Dockerfile is based of a python 3.11 image and pulls the latest version of Alyx from the github repository.
Additional packages installed are tools to build the `pyscopg2` package, which is required to connect to the postgres database, and the ssh service, which will allow to connect to the container later on.

### Command to build the container
Build Docker container:

    cd /home/olivier/Documents/PYTHON/00_IBL/iblalyx/deployment
    docker buildx build ./ \
        --platform linux/amd64 \
        --tag public.ecr.aws/p4h6o9n8/alyx:latest \
        --build-arg DATETIME=$(date +%Y-%m-%d-%H:%M:%S)

The buildx command is used to make sure the image is built for the right architecture (linux/amd64), even if the build is done on a different architecture (e.g. macOS).

### Pushing the container to ECR

    aws --profile ucl ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws/p4h6o9n8   
    docker push public.ecr.aws/p4h6o9n8/alyx:latest


### Running the container locally
    
    cd /home/olivier/Documents/PYTHON/00_IBL/iblalyx/deployment
    docker run  \
        --env-file environment.env \
        --platform linux/amd64 \
        -p 8000:8000 \
        public.ecr.aws/p4h6o9n8/alyx:latest

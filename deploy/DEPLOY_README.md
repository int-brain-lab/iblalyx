# Playbooks to spin up a new IBL Alyx instance

Here we are going to detail 3 scenarios:
- quick code update process inside the container
- container release process
- Full AWS instance creation process

## Quick code update process
From mbox, connect to the EC2 instance.
Get the bash command inside of the Docker: `docker-bash`
Run the following commands:
```shell
git pull
git  -C ./ibl_reports pull
```
Eventually apply some migrations
```shell
./manage.py makemigrations  # this should not make any changes. If it does, abort !!
./manage.py migrate
```
Restart the docker compose gracefully
```shell
exit
cd ~/iblalyx/deploy
docker compose restart
```

## Container release process

### Outline of the process:
1. **Build the container locally and test it**
2. **Push the container to ECR**
3. **Update the container on EC2**

### Detailed instructions:

1. Build the container locally and test it
    ```
    cd ./deploy/container
   ./buildrunlocal.sh
   ```
This will fist build the docker image, and then run it locally. 
The container runs a Django development server available at `localhost:8000`.

2. Push the container to ECR. We store this container in Elastic Container Registry (ECR) for public images.

First authenticate, this assumes that the profile `ucl` is set up in the AWS CLI.
   ```
   aws --profile ucl ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws/p4h6o9n8
   docker push public.ecr.aws/p4h6o9n8/alyx:latest
   ```
   
3. Update and start/restart the container on EC2
   ```
   docker pull public.ecr.aws/p4h6o9n8/alyx:latest
   cd ~/iblalyx/deploy
   docker compose up
   ```


## AWS instance creation process
- Create a new EC2 instance, with a volume of a few GB
- Update mbox ssh config file with the new instance IP
- Connect to the instance from mbox
- Clone IBL alyx repository and create the environment file with secrets:
   ```
   git clone https://github.com/int-brain-lab/iblalyx.git
   cd iblalyx/deploy
   cp environment_template.env environment.env
   ```
- Run the bootstrap script to prepare the instance
    ```
    sudo bash alyx_ec2_bootstrap.sh alyx-prod
    ```
-   create and start the container services:
    ```
    cd ~/iblalyx/deploy
    sudo docker compose up
    ```
# Playbooks to spin up a new IBL Alyx instance

Here we are going to detail 3 scenarios:
- quick code update process inside the container
- container release process
- Full AWS instance creation process

## Quick code update process
No migrations, just git pull inside the container


## Container release process
1. Build the container locally and test it
2. Push the container to ECR
3. Update the container on EC2

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
   # TODO: command to start/restart with docker-compose
   # TODO: first a first start, command to make sure the container restart on reboot
   ```


## AWS instance creation process
-   Create a new EC2 instance, Ubuntu and SSH into it 
-   run the convenience `alyx_ec2_bootstrap.sh` script to prepare the instnce
-   run the instructions above to update the container on EC2
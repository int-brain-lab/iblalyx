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
sudo docker compose restart
```

## Container release process



Note for the above command to work you need aws cli v2 installed (use aws --version to find current version). If necessary, follow
[these](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) to update.

On a fresh new EC2 instance.
   ```
   mkdir -p ~/Documents/PYTHON/iblalyx/deploy
   git clone https://github.com/int-brain-lab/iblalyx.git
   
   cd ~/iblalyx/deploy
   ansible-playbook ansible_setup_alyx_server.yml
   ```


## AWS instance creation process
- Create a new EC2 instance, with a volume of a few GB (easiest is to use the templates for alyx-prod or openalyx)
- Update mbox ssh config file with the new instance IP
- Connect to the instance from mbox
- Clone IBL alyx repository and create the environment file with secrets:
   ```
   git clone https://github.com/int-brain-lab/iblalyx.git
   cd iblalyx/deploy
   cp environment_template.env environment.env
   ```
- Run the bootstrap script to prepare the instance, need to specify the hostname and the rds security group name
    ```
    sudo bash alyx_ec2_bootstrap.sh alyx-prod alyx_rds
    ```
-   create and start the container services:
    ```
    cd ~/iblalyx/deploy
    sudo docker compose up
    ```

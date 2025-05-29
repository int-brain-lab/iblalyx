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




```shell

docker run \
  -it \
  --rm \
  --name alyx_certbot \
  -v /etc/letsencrypt:/etc/apache2/ssl \
  internationalbrainlab/alyx_apache:latest \
  certbot --apache --noninteractive --agree-tos --email admin@internationalbrainlab.org -d test.alyx.internationalbrainlab.org"
  /bin/bash
  
  
  
docker exec -it alyx certbot --apache --noninteractive --agree-tos --email admin@internationalbrainlab.org -d test.alyx.internationalbrainlab.org

```


 vi ~/Documents/PYTHON/alyx/deploy/.env
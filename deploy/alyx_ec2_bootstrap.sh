#!/bin/bash
# script to prepare a newly launched instance to run the ibl alyx docker image
# the hostname should be either of (alyx-prod, alyx-dev, openalyx), this is important for automated certificate renewals
# >>> sudo bash alyx_ec2_bootstrap.sh hostname
# the script will
# - setup a cron job to renew https certificate for the host/domain name
# - set the local timezone
# - install docker
# - add the ec2 instance IP address to the "alyx-rds" security groups
# - create a 'docker-bash' alias command in the .bashrc to open a shell in the running container


echo "NOTE: Installation log can be found in the directory the script is called from and named 'alyx_ec2_bootstrap_install.log'"
{
# check to make sure the script is being run as root (not ideal, Docker needs to run as root for IP logging)
if [ "$(id -u)" != "0" ]; then
  echo "Script needs to be run as root, exiting."
  exit 1
fi

# check on arguments passed, at least one is required to pick build env
if [ -z "$1" ]; then
    echo "Error: No argument supplied, script requires first argument for hostname (alyx-prod, alyx-dev, openalyx)"
    exit 1
fi

# Set vars
WORKING_DIR=/home/ubuntu/alyx-docker
LOG_DIR=/home/ubuntu/logs
EC2_REGION="eu-west-2"
IP_ADDRESS=$(ip route get 8.8.8.8 | awk -F"src " 'NR==1{split($2,a," ");print a[1]}')
DATE_TIME=$(date +"%Y-%m-%d %T")
SG_DESCRIPTION="${1}, ec2 instance, created: ${DATE_TIME}"
CERTBOT_CRON="30 1 1,15 * * docker exec alyx_con /bin/bash /home/ubuntu/iblalyx/crons/renew_docker_certs.sh ${1} > ${LOG_DIR}/cert_renew.log 2>&1"

echo "Creating relevant directories and log files..."
mkdir -p $WORKING_DIR
mkdir -p $LOG_DIR
touch "${LOG_DIR}/cert_renew.log"
chmod 666 "${LOG_DIR}/cert_renew.log"

echo "Setting hostname of instance..."
hostnamectl set-hostname "$1"

echo "Setting timezone to Europe\Lisbon..."
timedatectl set-timezone Europe/Lisbon

echo "Add Docker's official GPG key, setup for the docker stable repo"
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

echo "Update apt package index, install awscli docker, and allow apt to use a repository over HTTPS..."
apt-get -qq update
apt-get install -y \
  awscli \
  ca-certificates \
  containerd.io \
  docker-ce \
  docker-ce-cli \
  gnupg

echo "Testing docker..."
docker run hello-world

echo "Adding IP Address to 'alyx_rds' security group with unique description..."
aws ec2 authorize-security-group-ingress \
    --region=$EC2_REGION \
    --group-name alyx_rds \
    --ip-permissions IpProtocol=tcp,FromPort=5432,ToPort=5432,IpRanges="[{CidrIp=${IP_ADDRESS}/32,Description='${SG_DESCRIPTION}'}]"

cd $WORKING_DIR || exit 1

echo "Building out crontab entries..."
echo -e "${LOG_CRON}\n${CERTBOT_CRON}" >> temp_cron
crontab temp_cron # install new cron file
rm temp_cron # remove temp_cron file

echo "Adding alias to .bashrc..."
echo '' >> /home/ubuntu/.bashrc \
  && echo "# IBL Alias" >> /home/ubuntu/.bashrc \
  && echo "alias docker-bash='sudo docker exec --interactive --tty alyx_con /bin/bash'" >> /home/ubuntu/.bashrc

echo "Performing any remaining package upgrades..."
apt upgrade -y

echo "Instance will now reboot to ensure everything works correctly on a fresh boot."
sleep 10s
} | tee -a alyx_ec2_bootstrap_install.log

reboot

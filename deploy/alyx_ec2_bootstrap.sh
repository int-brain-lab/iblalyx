#!/bin/bash
set -e
# script to prepare a newly launched instance to run the ibl alyx docker image
# the hostname should be either of (alyx-prod, alyx-dev, openalyx), this is important for automated certificate renewals
# the rds_name should either be alyx_rds (for alyx-prod or alyx-dev) or openalyx_backend (for openalyx)
# >>> sudo bash alyx_ec2_bootstrap.sh hostname rds_name
# the script will
# - setup a cron job to renew https certificate for the host/domain name
# - set the local timezone
# - install docker
# - add the ec2 instance IP address to the "rds_name" security groups
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

# check on arguments passed, at least one is required to pick build env
if [ -z "$2" ]; then
    echo "Error: No argument supplied, script requires second argument for rds security group (alyx_rds, openalyx_backend)"
    exit 1
fi



# Set vars
HOSTNAME=$1
RDS_NAME=$2
HOME_DIR=/home/ubuntu  # this is the home directory of the EC2 instance - not the docker
WORKING_DIR=/home/ubuntu/alyx-docker
LOG_DIR=/var/log/apache2
EC2_REGION="eu-west-2"
IP_ADDRESS=$(ip route get 8.8.8.8 | awk -F"src " 'NR==1{split($2,a," ");print a[1]}')
DATE_TIME=$(date +"%Y-%m-%d %T")
SG_DESCRIPTION="${HOSTNAME}, ec2 instance, created: ${DATE_TIME}"

CRONTAB="# At 01:30 on day-of-month 1 and 15 we renew certificates
30 1 1,15 * * docker exec alyx /bin/bash /home/iblalyx/crons/renew_docker_certs.sh ${HOSTNAME} > ${LOG_DIR}/cert_renew.log 2>&1
# at 10am on Monday we rotate logs
0 10 * * 1 logrotate -vs  ${LOG_DIR}/logrotate.state ${HOME_DIR}/iblalyx/deploy/alyxlogrotate.conf"

echo "Creating relevant directories and log files..."
dd if=/dev/zero of=/home/ubuntu/spacer.bin bs=1 count=0 seek=1G  # this is a spacer file in case the system runs out of space
mkdir -p $LOG_DIR
chown -R www-data:www-data $LOG_DIR
mkdir -p $WORKING_DIR
chmod 600 ${HOME_DIR}/iblalyx/deploy/alyxlogrotate.conf
chown root:root ${HOME_DIR}/iblalyx/deploy/alyxlogrotate.conf

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

echo "Adding IP Address to '${RDS_NAME}' security group with unique description..."
aws ec2 authorize-security-group-ingress \
    --region=$EC2_REGION \
    --group-name $RDS_NAME \
    --ip-permissions IpProtocol=tcp,FromPort=5432,ToPort=5432,IpRanges="[{CidrIp=${IP_ADDRESS}/32,Description='${SG_DESCRIPTION}'}]"

cd $WORKING_DIR || exit 1

echo "Copying SSL certificates from S3..."
aws s3 cp s3://alyx-docker/fullchain.pem-"$HOSTNAME" /etc/letsencrypt/fullchain.pem
aws s3 cp s3://alyx-docker/privkey.pem-"$HOSTNAME" /etc/letsencrypt/privkey.pem

echo "Building out crontab entries..."
echo -e "${CRONTAB}" > temp_cron
crontab temp_cron # install new cron file
rm temp_cron # remove temp_cron file

echo "Adding alias to .bashrc..."
echo '' >> /home/ubuntu/.bashrc \
  && echo "# IBL Alias" >> /home/ubuntu/.bashrc \
  && echo "alias docker-bash='sudo docker exec --interactive --tty alyx /bin/bash'" >> /home/ubuntu/.bashrc

echo "Instance will now reboot to ensure everything works correctly on a fresh boot."
sleep 10s
} | tee -a alyx_ec2_bootstrap_install.log

reboot

#!/bin/bash
# A script to renew let's encrypt certificates from within a restricted dockerized AWS EC2 instance
# $1 determines the environment (alyx-prod, alyx-dev, openalyx)

# Set Vars
EC2_REGION="eu-west-2"
CERTBOT_SG="sg-07b6343c0d485e097"

echo "Checking arg passed for build environment, special case for alyx-dev..."
if [ -z "$1" ]; then
  echo "Error: No argument supplied, script requires first argument for build env (alyx-prod, alyx-dev, openalyx, etc)"
  exit 1
else
  if [ "${1}" == "alyx-dev" ]; then
    ALYX_URL="dev.alyx.internationalbrainlab.org"
  elif [ "${1}" == "alyx-prod" ]; then
    ALYX_URL="alyx.internationalbrainlab.org"
  elif [ "${1}" == "openalyx" ]; then
    ALYX_URL="openalyx.internationalbrainlab.org"
  else
    echo "Incorrect argument passed to script, exiting..."
    exit 1
  fi
  echo "ALYX_URL set to ${ALYX_URL}"
fi

echo "Retrieving instance-id from local metadata..."
EC2_INSTANCE_ID=$(wget -q -O - http://169.254.169.254/latest/meta-data/instance-id)
echo "EC2_INSTANCE_ID found: ${EC2_INSTANCE_ID}"

echo "Retrieving list of security groups attached to this instance..."
SG_IDS=$(aws ec2 describe-instances \
  --region=$EC2_REGION \
  --instance-id $EC2_INSTANCE_ID \
  --query "Reservations[].Instances[].SecurityGroups[].GroupId[]" \
  --output text)
echo "SG_IDS found: ${SG_IDS}"

# check if the "certbot_renewal" security group is in SG_IDS list
in_list=0
case $SG_IDS in *$CERTBOT_SG*) in_list=1 ;; esac

if [ $in_list -eq 0 ]; then
  echo "Adding the 'certbot_renewal' security group to the instance temporarily..."
  sg_ids_with_certbot_sg="${SG_IDS} ${CERTBOT_SG}"
  aws ec2 modify-instance-attribute \
    --region $EC2_REGION \
    --instance-id $EC2_INSTANCE_ID \
    --groups $sg_ids_with_certbot_sg
fi

echo "Attempting to renew certs..."
certbot --apache --noninteractive --agree-tos --email admin@internationalbrainlab.org -d $ALYX_URL
echo "Restarting apache"
/etc/init.d/apache2 restart

echo "Copying newly generated certs to AWS S3 bucket..."
aws s3 cp /etc/letsencrypt/archive/$ALYX_URL/fullchain1.pem s3://alyx-docker/fullchain.pem-$1
aws s3 cp /etc/letsencrypt/archive/$ALYX_URL/privkey1.pem s3://alyx-docker/privkey.pem-$1

if [ $in_list -eq 0 ]; then
  echo "Reverting security groups for instance..."
  aws ec2 modify-instance-attribute \
    --region $EC2_REGION \
    --instance-id $EC2_INSTANCE_ID \
    --groups $SG_IDS
else
  echo "Security Group for cerbot renewal was found assigned prior to script running and was not removed. Please manually remove the security group if it is no longer required."
fi

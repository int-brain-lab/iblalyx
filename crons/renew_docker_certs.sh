#!/bin/bash
# A script to renew let's encrypt certificates from within a restricted dockerized AWS EC2 instance

# Set Vars
EC2_REGION="eu-west-2"
CERTBOT_SG="sg-07b6343c0d485e097"

# Retrieve instance-id from local metadata
EC2_INSTANCE_ID=$(wget -q -O - http://169.254.169.254/latest/meta-data/instance-id)

# Get a list of security groups attached to this instance
SG_IDS=$(aws ec2 describe-instances \
  --region=$EC2_REGION \
  --instance-id $EC2_INSTANCE_ID \
  --query "Reservations[].Instances[].SecurityGroups[].GroupId[]" \
  --output text)

# check if the "certbot_renewal" security group is in SG_IDS list
in_list=0
case $SG_IDS in *$CERTBOT_SG*) in_list=1 ;; esac

# Add the "certbot_renewal" security group to the instance temporarily
if [ $in_list -eq 1 ]; then
  sg_ids_with_certbot="${SG_IDS} ${CERTBOT_SG}"
  aws ec2 modify-instance-attribute \
    --region $EC2_REGION \
    --instance-id $EC2_INSTANCE_ID \
    --groups $sg_ids_with_certbot
fi

# Renew certs
# TODO: determine the hostname with nslookup or pass along hostname with an argument
# certbot --apache --noninteractive --agree-tos --email admin@internationalbrainlab.org -d dev.alyx.internationalbrainlab.org
# certbot --apache --noninteractive --agree-tos --email admin@internationalbrainlab.org -d openalyx.internationalbrainlab.org
# certbot --apache --noninteractive --agree-tos --email admin@internationalbrainlab.org -d alyx.internationalbrainlab.org

# sudo certbot -q renew  || echo "Failed to renew certs"

# Revert security groups for instance
aws ec2 modify-instance-attribute \
  --region $EC2_REGION \
  --instance-id $EC2_INSTANCE_ID \
  --groups $SG_IDS
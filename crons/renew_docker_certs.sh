#!/bin/bash
set -e
# A script to renew let's encrypt certificates from within a restricted dockerized AWS EC2 instance
# $1 determines the environment (alyx-prod, alyx-dev, openalyx, or full domain name)
# $2 is the security group ID for certbot renewal (default is sg-07b6343c0d485e097)
# $3 is the AWS region (default is eu-west-2)

# Set Vars
if [ -z "$2" ]; then
  if [ -z "$CERTBOT_SG" ]; then
    echo "Error: No argument supplied and no CERTBOT_SG set, please set one of them."
    exit 1
  fi
else
  CERTBOT_SG=${2:-sg-07b6343c0d485e097}
fi
EC2_REGION=${3:-eu-west-2}

echo "Checking arg passed for build environment, special case for alyx-dev..."
if [ "${1}" == "alyx-dev" ]; then
  ALYX_URL="dev.alyx.internationalbrainlab.org"
elif [ "${1}" == "alyx-prod" ]; then
  ALYX_URL="alyx.internationalbrainlab.org"
elif [ "${1}" == "openalyx" ]; then
  ALYX_URL="openalyx.internationalbrainlab.org"
elif [ -z "$1" ]; then
  if [ -z "$APACHE_SERVER_NAME" ]; then
    echo "Error: No argument supplied and no APACHE_SERVER_NAME set, please set one of them."
    exit 1
  else
    ALYX_URL="${APACHE_SERVER_NAME}"
  fi
else
  ALYX_URL="${1}"
fi
echo "ALYX_URL set to ${ALYX_URL}"

echo "Retrieving instance-id from local metadata..."
if [ -z $EC2_INSTANCE_ID ]
then
  die() { status=$1; shift; echo "FATAL: $*"; exit $status; }
  EC2_INSTANCE_ID="`wget -q -O - http://169.254.169.254/latest/meta-data/instance-id || die "wget instance-id has failed: $?\"`"
fi

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
# If APACHE_SERVER_ADMIN is set, use it; otherwise, default to admin@domain
if [ -z "$APACHE_SERVER_ADMIN" ]; then
    domain=$(echo "$ALYX_URL" | awk -F. '{print $(NF-1)"."$NF}')
    email="admin@$domain"
else
    email="$APACHE_SERVER_ADMIN"
fi

certbot --apache --noninteractive --agree-tos --email "$email" -d $ALYX_URL

# check if bucket exists, if not skip copy
if aws s3 ls "s3://alyx-docker/" > /dev/null 2>&1; then
    echo "Copying newly generated certs to AWS S3 bucket..."
    aws s3 cp /etc/letsencrypt/live/$ALYX_URL/fullchain.pem s3://alyx-docker/fullchain.pem-$1
    aws s3 cp /etc/letsencrypt/live/$ALYX_URL/privkey.pem s3://alyx-docker/privkey.pem-$1
else
    echo "Bucket does not exist or command failed, skipping upload of certs."
fi

if [ $in_list -eq 0 ]; then
  echo "Reverting security groups for instance..."
  aws ec2 modify-instance-attribute \
    --region $EC2_REGION \
    --instance-id $EC2_INSTANCE_ID \
    --groups $SG_IDS
else
  echo "Security Group for cerbot renewal was found assigned prior to script running and was not removed. Please manually remove the security group if it is no longer required."
fi

echo "Restarting apache"
/etc/init.d/apache2 restart

#!/bin/bash
# This script is used to renew the SSL certificate for the Alyx server if certificates do not exist
# $APACHE_SERVER_NAME is the hostname, e.g. example.com, sub.example.com, or localhost

if [ "$APACHE_SERVER_NAME" = "localhost" ]; then
    echo "Deactivating SSL module for localhost."
    # Command to deactivate SSL module
    a2dismod ssl
    echo "Skipping certificate generation for localhost."
    exit 0
fi

# Fist check if the certificate files exist
if [ ! -f /etc/letsencrypt/live/$APACHE_SERVER_NAME/fullchain.pem ] || [ ! -f /etc/letsencrypt/live/$APACHE_SERVER_NAME/privkey.pem ] && [ -n $CERTBOT_SG ]; then
    echo "SSL certificate files do not exist. Proceeding with certificate generation"
    # Deactivate SSL module if it is enabled
    # a2dismod ssl
    echo "Generating self-signed SSL certificate for $APACHE_SERVER_NAME"
    # Create directories if they do not exist
    mkdir -p /etc/letsencrypt/live/$APACHE_SERVER_NAME
    openssl req -x509 -nodes -days 1 -newkey rsa:2048 \
        -keyout /etc/letsencrypt/live/$APACHE_SERVER_NAME/apache-selfsigned.key \
        -out /etc/letsencrypt/live/$APACHE_SERVER_NAME/apache-selfsigned.crt \
        -subj "/C=GB/ST=London/L=London/O=IBL/OU=IT/CN=${APACHE_SERVER_NAME}" &&
    # Generate a new SSL certificate using certbot
    # TODO Hostname variable should be set in the environment
    # Start apache server
    apache2ctl start
    rm -rf /etc/letsencrypt/live/$APACHE_SERVER_NAME

    /bin/bash /home/iblalyx/crons/renew_docker_certs.sh
    
    # Restart apache server to apply the new certificate
    apache2ctl stop
    # a2enmod ssl
fi

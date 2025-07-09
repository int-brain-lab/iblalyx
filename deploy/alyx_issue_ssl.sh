#!/bin/bash
# This script is used to create a new SSL certificate for the Alyx server if certificates do not exist
# $APACHE_SERVER_NAME is the hostname, e.g. example.com, sub.example.com, or localhost

if [ "$APACHE_SERVER_NAME" = "localhost" ]; then
    echo "Deactivating SSL module for localhost."
    # Command to deactivate SSL module
    a2dismod ssl
    echo "Skipping certificate generation for localhost."
    exit 0
fi

# Fist check if the certificate files exist
if { [ ! -f /etc/letsencrypt/live/$APACHE_SERVER_NAME/fullchain.pem ] || [ ! -f /etc/letsencrypt/live/$APACHE_SERVER_NAME/privkey.pem ]; } && [ -n "$CERTBOT_SG" ]; then
    echo "SSL certificate files do not exist. Proceeding with certificate generation"
    # To get apache running for the certbot challenge we first create a temporary self-signed certificate
    echo "Generating self-signed SSL certificate for $APACHE_SERVER_NAME"
    # Create directories if they do not exist
    mkdir -p /etc/letsencrypt/live/$APACHE_SERVER_NAME
    openssl req -x509 -nodes -days 1 -newkey rsa:2048 \
        -keyout /etc/letsencrypt/live/$APACHE_SERVER_NAME/apache-selfsigned.key \
        -out /etc/letsencrypt/live/$APACHE_SERVER_NAME/apache-selfsigned.crt \
        -subj "/C=GB/ST=London/L=London/O=IBL/OU=IT/CN=${APACHE_SERVER_NAME}" &&

    # Start apache server
    apache2ctl start
    rm -rf /etc/letsencrypt/live/$APACHE_SERVER_NAME
    # Generate a new SSL certificate using certbot
    /bin/bash /home/iblalyx/crons/renew_docker_certs.sh
    
    # Restart apache server to apply the new certificate (NB: server started by docker-compose)
    apache2ctl stop
fi

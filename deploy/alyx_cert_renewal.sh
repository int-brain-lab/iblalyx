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
if [ ! -f /etc/letsencrypt/live/$1/fullchain.pem ] || [ ! -f /etc/letsencrypt/live/$1/privkey.pem ] && [ -n $CERTBOG_SG ]; then
    echo "SSL certificate files do not exist. Proceeding with certificate generation"
    # Deactivate SSL module if it is enabled
    a2dismod ssl
    # Generate a new SSL certificate using certbot
    # TODO Hostname variable should be set in the environment
    # Start apache server
    apache2ctl start
    /bin/bash /home/iblalyx/crons/renew_docker_certs.sh
    
    # Restart apache server to apply the new certificate
    apache2ctl stop
    a2enmod ssl
fi

#!/bin/bash
source /var/www/alyx-main/venv/bin/activate
globus transfer e6db64de-791a-11ec-b2c3-1b99bfd4976a:/~/mnt/ibl/ ab2d064c-413d-11eb-b188-0ee0d5d9299f:/histology/ --recursive --sync-level mtime

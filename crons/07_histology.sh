#!/bin/bash
source /var/www/alyx-main/venv/bin/activate
globus transfer 3291ffa4-7ec6-11ec-9f32-ed182a728dff:/~/mnt/ibl/ ab2d064c-413d-11eb-b188-0ee0d5d9299f:/histology/ --recursive --sync-level mtime

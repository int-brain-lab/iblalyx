#!/bin/bash
ssh -i /home/ubuntu/.ssh/sdsc_alyx.pem -p 62022 datauser@ibl.flatironinstitute.org "source ~/Documents/PYTHON/envs/iblenv/bin/activate; python ~/Documents/github/iblalyx/scripts/process_logs.py $1 $2 > ~/s3_logs/s3_log.log 2>&1"

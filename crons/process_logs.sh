#!/bin/bash
ssh -i /home/ubuntu/.ssh/sdsc_alyx.pem -p 62022 datauser@ibl.flatironinstitute.org "source ~/Documents/PYTHON/envs/iblenv/bin/activate; python ~/Documents/PYTHON/iblalyx/scripts/process_logs.py $1 $2 > ~/s3_logs/s3_log.log 2>&1" && curl -fsS -m 10 --retry 5 -o /dev/null https://hc-ping.com/d067db71-0783-475e-8159-cf81fee57c6b

# to check a specific time: cat /backups/cpu_info.log | grep 2021-12-17T14:34 -A 9
#!/bin/bash
set -e
date +%Y-%m-%dT%H:%M:%S >> /backups/cpu_info.log
top -b -n 2 | grep -A6 -B5 PID |tail -12 >> /backups/cpu_info.log

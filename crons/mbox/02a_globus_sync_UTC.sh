# docker exec -i ibl_alyx_apache bash -s < 02a_globus_sync_UTC.sh
# docker exec -i ibl_alyx_apache bash -s < /home/ubuntu/Documents/PYTHON/iblalyx/crons/mbox/02a_globus_sync_UTC.sh

echo "Start synchronisation for UTC time zone"

./manage.py files bulksync --lab=cortexlab
./manage.py files bulktransfer --lab=cortexlab

./manage.py files bulksync --lab=mainenlab
./manage.py files bulktransfer --lab=mainenlab

./manage.py files bulksync --lab=mrsicflogellab
./manage.py files bulktransfer --lab=mrsicflogellab

./manage.py files bulksync --lab=hoferlab
./manage.py files bulktransfer --lab=hoferlab

./manage.py files bulksync --lab=hausserlab
./manage.py files bulktransfer --lab=hausserlab

sleep 900

./manage.py files bulksync --lab=cortexlab
./manage.py files bulksync --lab=mainenlab
./manage.py files bulksync --lab=mrsicflogellab
./manage.py files bulksync --lab=hoferlab
./manage.py files bulksync --lab=hausserlab

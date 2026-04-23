# docker exec -i ibl_alyx_apache bash -s < 02b_globus_sync_EST.sh

echo "Start synchronisation for EST time zone"

./manage.py files bulksync --lab=churchlandlab
./manage.py files bulktransfer --lab=churchlandlab

./manage.py files bulksync --lab=wittenlab
./manage.py files bulktransfer --lab=wittenlab

./manage.py files bulksync --lab=angelakilab
./manage.py files bulktransfer --lab=angelakilab

./manage.py files bulksync --lab=zadorlab
./manage.py files bulktransfer --lab=zadorlab

sleep 900

./manage.py files bulksync --lab=churchlandlab
./manage.py files bulksync --lab=wittenlab
./manage.py files bulksync --lab=angelakilab
./manage.py files bulksync --lab=zadorlab

import globus_sdk
from iblutil.io import params
import ibllib.io.globus as globus

from data.models import FileRecord, DatasetType, Dataset

# 1. Change dataset type records
ds_type = DatasetType.objects.filter(name__startswith='_ibl_trials.laser')

# Check we have the correct dataset types (fixtures also need changing at this point)
assert len(ds_type) == 2
if 'laser_' in ds_type[0].name:
    assert ds_type[0].name == '_ibl_trials.laser_probability'

    # Rename them
    ds_type[0].filename_pattern = '*trials.laserProbability*'
    ds_type[0].name = '_ibl_trials.laserProbability'
    ds_type[0].save()

if 'laser_' in ds_type[1].name:
    ds_type[1].filename_pattern = '*trials.laserStimulation*'
    ds_type[1].name = '_ibl_trials.laserStimulation'
    ds_type[1].save()

# 2. Change dataset name
dsets = Dataset.objects.filter(dataset_type__name='_ibl_trials.laserProbability')
for dset in dsets:
    dset.name = '_ibl_trials.laserProbability.npy'
    dset.save()
dsets = Dataset.objects.filter(dataset_type__name='_ibl_trials.laserStimulation')
for dset in dsets:
    dset.name = '_ibl_trials.laserStimulation.npy'
    dset.save()

# 3. Patch file records
# Set up Globus
str_app = 'globus/admin'
client_id = params.read(str_app).GLOBUS_CLIENT_ID
tc = globus.login_auto(globus_client_id=client_id, str_app=str_app)
records = FileRecord.objects.filter(dataset__name='_ibl_trials.laserProbability.npy')
for record in records:
    globus_path = record.data_repository.globus_path
    rel_path_old = record.relative_path
    if 'laser_prob' not in rel_path_old:
        print('skipping ' + rel_path_old)
        continue
    rel_path_new = rel_path_old.replace('laser_prob', 'laserProb')
    # Rename file on disk
    print(f'{rel_path_old} -> {rel_path_new}')
    ep_id = str(record.data_repository.globus_endpoint_id)
    tc.operation_rename(ep_id, oldpath=globus_path + rel_path_old,
                        newpath=globus_path + rel_path_new)

    # Raname file record
    record.relative_path = rel_path_new
    record.save()

records = FileRecord.objects.filter(dataset__name='_ibl_trials.laserStimulation.npy')
for record in records:
    globus_path = record.data_repository.globus_path
    rel_path_old = record.relative_path
    if 'laser_prob' not in rel_path_old:
        print('skipping ' + rel_path_old)
        continue
    rel_path_new = rel_path_old.replace('laser_stim', 'laserStim')
    # Rename file on disk
    print(f'{rel_path_old} -> {rel_path_new}')
    ep_id = str(record.data_repository.globus_endpoint_id)
    tc.operation_rename(ep_id, oldpath=globus_path + rel_path_old,
                        newpath=globus_path + rel_path_new)

    # Raname file record
    record.relative_path = rel_path_new
    record.save()

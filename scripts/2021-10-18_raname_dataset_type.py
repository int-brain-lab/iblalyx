"""This script renames all instances of a given dataset type.  This includes changing the
dataset type name and file pattern, dataset name, file record relative path and actual filename
on disk, using the Globus rename operation.

In this script two dataset types were renamed (hence much of the code is duplicated):
    _ibl_trials.laser_probability -> _ibl_trials.laserProbability
    _ibl_trials.laser_stimulation -> _ibl_trials.laserStimulation
"""
from pathlib import Path
import warnings

import globus_sdk
from iblutil.io import params
from one.alf.spec import _dromedary
import ibllib.io.globus as globus

from data.models import FileRecord, DatasetType, Dataset

# 1. Change dataset type records
ds_type = DatasetType.objects.filter(name__startswith='_ibl_trials.laser')

# Check we have the correct dataset types (fixtures also need changing at this point)
assert len(ds_type) == 2
if 'laser_' in ds_type[0].name:  # may already be done in fixtures
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
dsets = Dataset.objects.filter(dataset_type__name__startswith='_ibl_trials.laser')
for dset in dsets:
    attr = dset['name'].split('.')[1]
    if 'laser_' in attr:
        dset.name = dset['name'].replace(attr, _dromedary(attr))  # e.g. laser_stim -> laserStim
        dset.save()

# 3. Patch file records
# Set up Globus
str_app = 'globus/admin'
try:
    client_id = params.read(str_app).GLOBUS_CLIENT_ID
    tc = globus.login_auto(globus_client_id=client_id, str_app=str_app)
except AttributeError:
    # Store client ID in params file for next time
    client_id = input('Enter Globus client ID:').strip()
    pars = params.read(str_app).set('GLOBUS_CLIENT_ID', client_id)
    params.write(str_app, pars)
    tc = globus.login_auto(globus_client_id=client_id, str_app=str_app)
except (FileNotFoundError, ValueError):
    client_id = input('Enter Globus client ID:').strip()
    globus.setup(client_id, str_app)
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
    if record.data_repository.globus_is_personal:
        try:
            r = tc.operation_rename(ep_id, oldpath=globus_path + rel_path_old,
                                    newpath=globus_path + rel_path_new)
            assert r.data['code'] == 'FileRenamed'
        except globus_sdk.TransferAPIError as ex:
            if ex.code == 502 or ex.code == 409:
                warnings.warn(ex.message)
                print(f'skipping {rel_path_old} due to error')
                continue
            # Check whether already renamed
            if record.exists:
                p = Path(globus_path + rel_path_old).parent.as_posix()
                r = tc.operation_ls(ep_id, path=p, filter='name:' + Path(rel_path_new).name)
    # Raname file record
    record.relative_path = rel_path_new
    record.save()

records = FileRecord.objects.filter(dataset__name='_ibl_trials.laserStimulation.npy')
for record in records:
    globus_path = record.data_repository.globus_path
    rel_path_old = record.relative_path
    if 'laser_stim' not in rel_path_old:
        print('skipping ' + rel_path_old)
        continue
    rel_path_new = rel_path_old.replace('laser_stim', 'laserStim')
    # Rename file on disk
    print(f'{rel_path_old} -> {rel_path_new}')
    ep_id = str(record.data_repository.globus_endpoint_id)
    if record.data_repository.globus_is_personal:
        try:
            r = tc.operation_rename(ep_id, oldpath=globus_path + rel_path_old,
                                    newpath=globus_path + rel_path_new)
            assert r.data['code'] == 'FileRenamed'
        except globus_sdk.TransferAPIError as ex:
            if ex.code == 502 or ex.code == 409:
                """Unfortunately on 502 and 409 exceptions the error isn't caught here (maybe 
                it's a different exception class?).  These 'Globus not connected' errors are 
                extremely common and sporadic.  For now I just re-run this loop until they're 
                all patched"""
                warnings.warn(ex.message)
                print(f'skipping {rel_path_old} due to error')
                continue
            # Check whether already renamed
            if record.exists:
                p = Path(globus_path + rel_path_old).parent.as_posix()
                r = tc.operation_ls(ep_id, path=p, filter='name:' + Path(rel_path_new).name)
    # Raname file record
    record.relative_path = rel_path_new
    record.save()

# Adapted from: https://github.com/int-brain-lab/ibldevtools/blob/69a7f4bd6a3d54efd7434a238a3a4453823832e2/miles/2022-01-17-alyx_trials_table_patch.py#L146-L154

"""
Given a set of eids, tries to generate trials table for those sessions, registers those
and removes the old individual files
"""

from pathlib import Path
import logging
from subprocess import Popen, PIPE, STDOUT

import pandas as pd
import numpy as np
import globus_sdk as globus

from data.views import RegisterFileViewSet
from data.models import Dataset, DataRepository, FileRecord
from actions.models import Session

from one.alf import spec, io as alfio, files as alfiles
from one.alf.exceptions import ALFObjectNotFound
from iblutil.util import Bunch
from iblutil.io import hashfile, params

# Variables
root_path = Path('/mnt/ibl')
bucket_name = 's3://ibl-brain-wide-map-private'
alyx_user = 'julia.huntenburg'
# Sessions for which to try
eids = list(pd.read_csv(root_path.joinpath('aggregates', 'trials', 'no_trials_table.csv'), index_col=0)['eid'])
# Trials attributes that need to exist to create the trials table
attributes = [
    'intervals',
    'goCue_times',
    'response_times',
    'choice',
    'stimOn_times',
    'contrastLeft',
    'contrastRight',
    'feedback_times',
    'feedbackType',
    'rewardVolume',
    'probabilityLeft',
    'firstMovement_times'
]
attr = [f'^{x}$' for x in attributes]

# Prepare logger
logger = logging.getLogger('ibllib')
logger.setLevel(logging.INFO)
handler = logging.handlers.RotatingFileHandler(root_path.joinpath('create_trials_table.log'),
                                               maxBytes=(1024 * 1024 * 256), )
logger.addHandler(handler)

# Safety measure to ensure script ran to completion. This file should be empty before opening.
incomplete = root_path.joinpath('trials_patch_sanity_check')
if incomplete.exists():
    with open(incomplete, 'r') as f1:
        eid = f1.readline()
    assert eid == '', f'Session {eid} from previous run was incompletely processed'
f1 = open(incomplete, 'w')


def log_subprocess_output(pipe, log_function=print):
    for line in iter(pipe.readline, b''):
        log_function(line.decode().strip())


def login_auto(globus_client_id, str_app='globus/default'):
    token = params.read(str_app, {})
    required_fields = {'refresh_token', 'access_token', 'expires_at_seconds'}
    if not (token and required_fields.issubset(token.as_dict())):
        raise ValueError("Token file doesn't exist, run ibllib.io.globus.setup first")
    client = globus.NativeAppAuthClient(globus_client_id)
    client.oauth2_start_flow(refresh_tokens=True)
    authorizer = globus.RefreshTokenAuthorizer(token.refresh_token, client)
    return globus.TransferClient(authorizer=authorizer)


# Log into globus
gtc = login_auto('525cc517-8ccb-4d11-8036-af332da5eafd')

sessions = Session.objects.filter(id__in=eids)
for i, ses in enumerate(sessions):
    f1.write(str(ses.id))
    f1.flush()
    logger.info(f'Processing session {ses.id}, {i}/{sessions.count()}')
    alf_path = root_path.joinpath(
        ses.subject.lab.name,
        'Subjects',
        ses.subject.nickname,
        ses.start_time.strftime('%Y-%m-%d'),
        f'{ses.number:03d}',
        'alf'
    )
    if not alf_path.exists():
        logger.error(f"Alf path doesn't exist for {ses.id}")
        continue
    try:
        trials = alfio.load_object(alf_path, 'trials', attribute=attr, timescale=None, wildcards=False, short_keys=True)
    except ALFObjectNotFound:
        logger.error(f'Could not load trials objects for session {ses.id}')
        continue
    try:
        # Check dimensions of trials object
        assert alfio.check_dimensions(trials) == 0, 'Trials dimensions mismatch'
        # Check if all expected keys are present
        assert sorted(trials.keys()) == sorted(attributes), 'Missing attributes'
        # Check all columns are numpy arrays and not zero length
        assert all(
            isinstance(x, np.ndarray) and x.size > 0 for x in trials.values()), 'Not all attributes are numpy arrays'
        # Create and save table
        trials_df = trials.to_df()
        trials_df.index.name = 'trial_#'
        filename = spec.to_alf(object='trials', namespace='ibl', attribute='table', extension='pqt')
        fullfile = alf_path.joinpath(filename)
        trials_df.to_parquet(fullfile)
        assert fullfile.exists(), f'Failed to save to {fullfile}'
        assert not pd.read_parquet(fullfile).empty, f'Failed to read {fullfile}'

        # Register file in database
        logger.info(f'...registering {fullfile}')
        dataset_record = Bunch({'data': {}})
        dataset_record.data = {
            'name': f'flatiron_{ses.lab}',
            'path': '/'.join([ses.subject.nickname, ses.start_time.strftime('%Y-%m-%d'), f'{ses.number:03d}']),
            'labs': f'{ses.lab}',
            'hashes': hashfile.md5(fullfile),
            'filesizes': str(fullfile.stat().st_size),
            'server_only': True,
            'filenames': f'alf/{filename}',
            'created_by': alyx_user
        }
        r = RegisterFileViewSet().create(dataset_record)
        assert r.status_code == 201
        assert len(r.data) == 1
        # Rename file: add UUID
        did = r.data[0]['id']
        newfile = fullfile.rename(alfiles.add_uuid_string(fullfile, did))
        assert newfile.exists(), "Failed to save renamed file"
        # Create new file record for AWS
        record = {
            'dataset': Dataset.objects.get(id=did),
            'data_repository': DataRepository.objects.get(name=f'aws_{ses.lab}'),
            'relative_path': alfiles.get_alf_path(fullfile).replace(f'{ses.lab}/Subjects', '').strip('/'),
            'exists': True
        }
        _ = FileRecord.objects.get_or_create(**record)

        # De-register old files
        datasets = Dataset.objects.filter(session=ses, name__startswith='_ibl_trials')
        ddata = None
        logger.info('...de-registering datasets')
        for dataset in datasets:
            if dataset.tags.filter(protected=True).count():
                logger.warning(f'...skipping protected dataset {dataset.name}')
                continue
            if not any(x == dataset.name.split('.')[1] for x in attributes):
                continue  # Not in trials table
            for fr in dataset.file_records.all():
                if fr.data_repository.name.endswith('SR'):
                    if not ddata:
                        ddata = globus.DeleteData(gtc, fr.data_repository.globus_endpoint_id, recursive=False)
                    if fr.exists:
                        logger.info(f'...DELETE: {fr.data_repository.globus_path}{fr.relative_path}')
                        ddata.add_item(fr.data_repository.globus_path + fr.relative_path)
                    fr.exists = False
                elif fr.data_repository.name.startswith('flatiron'):
                    local_path = root_path.joinpath(fr.data_repository.globus_path.strip('/'), fr.relative_path)
                    local_path = alfiles.add_uuid_string(local_path, dataset.pk)
                    logger.info(f'...DELETE: {local_path}')
                    local_path.unlink()
                elif fr.data_repository.name.startswith('aws'):
                    bucket_name = fr.data_repository.json['bucket_name']
                    if not bucket_name.startswith('s3:'):
                        bucket_name = 's3://' + bucket_name
                else:
                    fr.exists = False
                fr.save()
            dataset.delete()
        if ddata and ddata.get('DATA', False):
            logger.info('...submitting delete')
            delete_result = gtc.submit_delete(ddata)
        # Delete from AWS
        print('Syncing AWS')
        dst_dir = bucket_name.strip('/') + '/data/' + alfiles.get_alf_path(alf_path)
        cmd = ['aws', 's3', 'sync', alf_path.as_posix(), dst_dir, '--delete', '--profile', 'ibladmin', '--no-progress']
        process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
        with process.stdout:
            log_subprocess_output(process.stdout, logger.info)
    except AssertionError as ex:
        logger.error(f'ERROR for {ses.id}: {ex}')
    f1.truncate(0)
    f1.seek(0)
f1.close()
incomplete.unlink()

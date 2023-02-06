# Adapted from
# https://github.com/int-brain-lab/ibldevtools/blob/master/olivier/archive/2022/2022-03-14_trials_tables.py
# https://github.com/int-brain-lab/ibldevtools/blob/master/miles/2022-01-17-alyx_trials_table_patch.py
# https://github.com/int-brain-lab/ibldevtools/blob/master/miles/2022-12-19_register-zainab-aggregates.py
"""
Generate per subject trials aggregate files for all culled subjects that have at least one session with an ibl project
and ibl task protocol.
1. Check if all sessions have trials tables. For those that don't, try to generate them.
   Log if it's not possible and skip those sessions.
2. Check for which subjects trial aggregate files need to be generated or updated
   (using hash of individual dataset uuids and hashes)
   a. If file exists and does not need updating, do nothing.
   b. If this is the first version of the file, generate and register dataset, create file records, sync to AWS
   c. If original file is protected, create and register new revision of dataset.
   d. If original file is not protected, overwrite it, update hash and file size of dataset.
3. Sync to AWS.
"""

'''
===========
SETTING UP
===========
'''

from django.db.models import Count, Q
from actions.models import Session
from subjects.models import Subject
from data.views import RegisterFileViewSet
from data.models import Dataset, DatasetType, DataFormat, DataRepository, FileRecord, Revision
from misc.models import LabMember

import logging
import datetime
import time
import hashlib
import traceback
from pathlib import Path
from subprocess import Popen, PIPE, STDOUT

import pandas as pd
import numpy as np
import globus_sdk as globus

from one.alf import spec, io as alfio, files as alfiles
from one.alf.exceptions import ALFObjectNotFound
from iblutil.util import Bunch
from iblutil.io import hashfile, params

# Settings
root_path = Path('/mnt/ibl')
output_path = Path('/mnt/ibl/aggregates/')
collection = 'Subjects'
file_name = '_ibl_subjectTrials.table.pqt'
alyx_user = 'julia.huntenburg'
version = 1.0
dry = False

# Set up
output_path.mkdir(exist_ok=True, parents=True)
alyx_user = LabMember.objects.get(username=alyx_user)
today_revision = datetime.datetime.today().strftime('%Y-%m-%d')

# Attributes required for trials table
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
today = datetime.datetime.today().strftime('%Y%m%d')
logger = logging.getLogger('ibllib')
logger.setLevel(logging.INFO)
handler = logging.handlers.RotatingFileHandler(output_path.joinpath(f'subjectTrials_{today}.log'),
                                               maxBytes=(1024 * 1024 * 256), )
logger.addHandler(handler)


# Functions
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


# Set up dictionaries to catch errors or other logs
status = {}
status_agg = {}

"""
=====================
SESSION TRIALS TABLES
=====================
"""
# Find sessions with ibl task protocol that don't have trials tables, but have other trials data, and try to create them
sessions = Session.objects.filter(task_protocol__icontains='ibl').exclude(subject__nickname__icontains='test')
sessions = sessions.annotate(
    trials_table_count=Count('data_dataset_session_related',
                             filter=Q(data_dataset_session_related__name='_ibl_trials.table.pqt')),
    trials_count=Count('data_dataset_session_related',
                       filter=Q(data_dataset_session_related__name__icontains='_ibl_trials')))
to_create = sessions.filter(trials_table_count=0).exclude(trials_count=0)

if to_create.count() > 0:
    if dry:
        logger.info(f'DRY RUN: WOULD CREATE TRIALS TABLES FOR {to_create.count()} SESSIONS')
    else:
        logger.info(f'CREATING TRIALS TABLES FOR {to_create.count()} SESSIONS')
    gtc = login_auto('525cc517-8ccb-4d11-8036-af332da5eafd')
    for session in to_create:
        logger.info(f'Session {session.id}')
        alf_path = root_path.joinpath(session.subject.lab.name, "Subjects", session.subject.nickname,
                                      session.start_time.strftime('%Y-%m-%d'), f'{session.number:03d}', 'alf')
        # Check if alf folder exists
        if not alf_path.exists():
            logger.error(f"...ERROR: Alf path doesn't exist")
            status[f'{session.id}'] = 'ERROR: Alf path does not exist'
            continue
        try:
            # Try to load the required attributes
            trials = alfio.load_object(alf_path, 'trials', attribute=attr, timescale=None,
                                       wildcards=False, short_keys=True)
        except ALFObjectNotFound:
            logger.error(f'...ERROR: Could not load all attributes')
            status[f'{session.id}'] = 'ERROR: Could not load all trials attributes'
            continue
        except AssertionError as e:
            str_err = traceback.format_exc()
            if "multiple object trials with the same attribute" in str_err:
                logger.error("...ERROR: Mulitple object trials with the same attribute")
                status[f'{session.id}'] = 'ERROR: Mulitple object trials with the same attribute'
                continue
        try:
            # Check dimensions of trials object
            assert alfio.check_dimensions(trials) == 0, 'Dimensions mismatch trials attributes'
            # Check if all expected keys are present
            assert sorted(trials.keys()) == sorted(attributes), 'Missing trials attributes'
            # Check all columns are numpy arrays and not zero length
            assert all(
                isinstance(x, np.ndarray) and x.size > 0
                for x in trials.values()), 'Not all trials attributes are arrays'
            # Create and save table
            trials_df = trials.to_df()
            trials_df.index.name = 'trial_#'
            filename = spec.to_alf(object='trials', namespace='ibl', attribute='table', extension='pqt')
            fullfile = alf_path.joinpath(filename)
            if dry:
                logger.info(f'...DRY RUN: not saving {fullfile}')
                status[f'{session.id}'] = 'DRY RUN: would create trials table'
                continue

            trials_df.to_parquet(fullfile)
            assert fullfile.exists(), f'Failed to save to {fullfile}'
            assert not pd.read_parquet(fullfile).empty, f'Failed to read {fullfile}'

            # Register file in database
            logger.info(f'...registering {fullfile}')
            dataset_record = Bunch({'data': {}})
            dataset_record.data = {
                'name': f'flatiron_{session.lab}',
                'path': '/'.join([session.subject.nickname, session.start_time.strftime('%Y-%m-%d'),
                                  f'{session.number:03d}']),
                'labs': f'{session.lab}',
                'hashes': hashfile.md5(fullfile),
                'filesizes': str(fullfile.stat().st_size),
                'server_only': True,
                'filenames': f'alf/{filename}',
                'created_by': alyx_user.username
            }
            r = RegisterFileViewSet().create(dataset_record)
            assert r.status_code == 201, f'Failed to register {fullfile}'
            assert len(r.data) == 1, f'Failed to register {fullfile}'
            # Rename file: add UUID
            did = r.data[0]['id']
            newfile = fullfile.rename(alfiles.add_uuid_string(fullfile, did))
            assert newfile.exists(), f"Failed to save renamed file {newfile}"
            # Create new file record for AWS
            record = {
                'dataset': Dataset.objects.get(id=did),
                'data_repository': DataRepository.objects.get(name=f'aws_{session.lab}'),
                'relative_path': alfiles.get_alf_path(fullfile).replace(f'{session.lab}/Subjects}', '').strip('/'),
                'exists': False
            }
            try:
                _ = FileRecord.objects.get_or_create(**record)
            except BaseException as e:
                logger.error(f'...ERROR: Failed to create AWS file record: {e}')
                status[f'{session.id}'] = 'ERROR: Failed to create AWS file record trials.table.pqt'
                continue

            # De-register individual attributes files
            datasets = Dataset.objects.filter(session=session, name__startswith='_ibl_trials')
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
            # Deleting files on local servers via globus
            if ddata and ddata.get('DATA', False):
                logger.info('...submitting delete on local server via globus')
                delete_result = gtc.submit_delete(ddata)
            # Deleting files from AWS
            logger.info('...syncing delete with AWS')
            dst_dir = bucket_name.strip('/') + '/data/' + alfiles.get_alf_path(alf_path)
            cmd = ['aws', 's3', 'sync', alf_path.as_posix(), dst_dir, '--delete', '--profile', 'ibladmin',
                   '--no-progress']
            process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
            with process.stdout:
                log_subprocess_output(process.stdout, logger.info)
            process.wait()
            if process.returncode != 0:
                logger.error(f'...ERROR: Failed to delete files from AWS')
                status[f'{session.id}'] = 'ERROR: Failed to delete trials files from AWS'
                continue
            status[f'{session.id}'] = 'SUCCESS: created trials table'
        except BaseException as ex:
            logger.error(f'...ERROR: {ex}')
            status[f'{session.id}'] = f'ERROR: {ex}'

# Save information about trials table creation as csv
status = pd.DataFrame.from_dict(status, orient='index', columns=['status'])
status.insert(0, 'eid', status.index)
status.reset_index(drop=True, inplace=True)
status.to_csv(output_path.joinpath('trials_table_status.csv'), index=False)

""""
========================
SUBJECT AGGREGATE TABLES
========================
"""
# Now find all culled subjects with at least one session in an ibl project
sessions = Session.objects.filter(project__name__icontains='ibl')
subjects = Subject.objects.filter(id__in=sessions.values_list('subject'), cull__isnull=False
                                  ).exclude(nickname__icontains='test')
# Also make sure to only keep subjects that have at least one session with ibl task protocol and a trials table
sessions = Session.objects.filter(subject__in=subjects, task_protocol__icontains='ibl')
sessions = sessions.annotate(
    trials_table_count=Count('data_dataset_session_related',
                             filter=Q(data_dataset_session_related__name='_ibl_trials.table.pqt')))
sessions = sessions.exclude(trials_table_count=0)
subjects = Subject.objects.filter(id__in=sessions.values_list('subject'))

# dataset format, type and repos
dataset_format = DataFormat.objects.get(name='parquet')
dataset_type = DatasetType.objects.get(name='subjectTrials.table')
aws_repo = DataRepository.objects.get(name='aws_aggregates')
fi_repo = DataRepository.objects.get(name='flatiron_aggregates')

# Go through subjects and check if aggregate needs to be (re)created
logger.info('\n')
logger.info(f' {subjects.count()} SUBJECTS')
for i, sub in enumerate(subjects):
    try:
        print(f'{i}/{subjects.count()} {sub.nickname}')
        logger.info(f'Subject {sub.nickname} {sub.id}')
        # Find all sessions of this subject
        sub_sess = Session.objects.filter(subject=sub, task_protocol__icontains='ibl')
        # First create hash and check if aggregate needs to be (re)created
        trials_ds = Dataset.objects.filter(session__in=sub_sess, name='_ibl_trials.table.pqt', default_dataset=True)
        trials_ds = trials_ds.order_by('hash')
        hash_str = ''.join([str(item) for pair in trials_ds.values_list('hash', 'id') for item in pair]).encode('utf-8')
        new_hash = hashlib.md5(hash_str).hexdigest()
        revision = None  # Only set if making a new revision is required
        # Check if this dataset exists
        # Todo: This will only work once generic foreign key is added and datasets are linked to subjects
        ds = Dataset.objects.filter(session__subject=sub, name=file_name, default_dataset=True)
        # If there is more than one default dataset, something is wrong
        if ds.count() > 1:
            logger.error(f'...ERROR: more than one default dataset')
            status_agg[f'{sub.id}'] = 'ERROR: more than one default dataset'
            continue
        # If there is exactly one default dataset, check if it needs updating
        elif ds.count() == 1:
            if ds.first().revision is None:
                out_file = output_path.joinpath(collection, sub.lab.name, sub.nickname, file_name)
            else:
                out_file = output_path.joinpath(collection, sub.lab.name, sub.nickname, ds.first().revision, file_name)
            # See if the file exists on disk (we are on SDSC so need to check with uuid in name)
            # If yes, create the expected hash and try to compare to the hash of the existing file
            if alfiles.add_uuid_string(out_file, ds.first().pk).exists():
                try:
                    old_hash = ds.first().json['aggregate_hash']
                except TypeError:
                    # If the json doesn't have the hash, just set it to None, we recreate the file in this case
                    old_hash = None
                # If the hash is the same we don't need to do anything
                if old_hash == new_hash:
                    logger.info(f'...aggregate exists and is up to date')
                    status_agg[f'{sub.id}'] = 'EXIST: aggregate exists, hash match'
                    continue
                else:
                    # Otherwise check if the file is protected, if yes, create a revision, otherwise overwrite
                    if ds.first().protected:
                        logger.info(f'...aggregate already exists but is protected, hash mismatch, creating revision')
                        status_agg[f'{sub.id}'] = 'REVISION: aggregate exists protected, hash mismatch'
                        # Make revision other than None and add revision to file path
                        revision, _ = Revision.objects.get_or_create(name=today_revision)
                        if ds.first().revision is None:
                            out_file = out_file.parent.joinpath(f"#{today_revision}#", out_file.name)
                        else:
                            # If the current default is already a revision, remove the revision part of the path
                            out_file = out_file.parent.parent.joinpath(f"#{today_revision}#", out_file.name)
                    else:
                        logger.info(f'...aggregate already exists but is not protected, hash mismatch, overwriting')
                        status_agg[f'{sub.id}'] = 'OVERWRITE: aggregate exists not protected, hash mismatch'
                        # Add the uuid to the out file to overwrite the current file
                        out_file = alfiles.add_uuid_string(out_file, ds.first().pk)
            # If the dataset entry exist but the dataset cannot be found on disk, just recreate the dataset
            else:
                logger.info(f'...dataset entry exists but file is missing on disk, creating new')
                status_agg[f'{sub.id}'] = 'CREATE: aggregate dataset entry exists, file missing'
                # Here, too, update the file name with the uuid to create the file on disk
                out_file = alfiles.add_uuid_string(out_file, ds.first().pk)
        # If no dataset exists yet, create it
        elif ds.count() == 0:
            out_file = output_path.joinpath(collection, sub.lab.name, sub.nickname, file_name)
            logger.info(f'...aggregate does not yet exist, creating.')
            status_agg[f'{sub.id}'] = 'CREATE: aggregate does not exist'

        # If dry run, stop here
        if dry:
            logger.info(f'...DRY RUN would create {out_file}')
            continue
        # Create aggregate dataset and save to disk
        all_trials = []
        for t in trials_ds:
            # load trials table
            alf_path = root_path.joinpath(sub.lab.name, 'Subjects', t.file_records.filter(
                data_repository__name__startswith='flatiron').first().relative_path
                                          ).parent
            trials = alfio.load_object(alf_path, 'trials', attribute='table', short_keys=True)
            trials = trials.to_df()

            # Add to list of trials for subject
            trials['session'] = str(t.session.id)
            trials['session_start_time'] = t.session.start_time
            trials['session_number'] = t.session.number
            trials['task_protocol'] = t.session.task_protocol
            all_trials.append(trials)

        # Concatenate trials from all sessions for subject and save
        df_trials = pd.concat(all_trials, ignore_index=True)
        out_file.parent.mkdir(parents=True, exist_ok=True)
        df_trials.to_parquet(out_file)
        assert out_file.exists(), f'Failed to save to {out_file}'
        assert not pd.read_parquet(out_file).empty, f'Failed to read {out_file}'
        logger.info(f"...Saved {out_file}")

        # Get file size and hash which we need in any case
        file_hash = hashfile.md5(out_file)
        file_size = out_file.stat().st_size
        # If we overwrote an existing file, update hashes and size in the dataset entry
        if ds.count() == 1 and revision is None:
            ds.update(hash=file_hash, file_size=file_size, json={'aggregate_hash': new_hash})
            logger.info(f"...Updated hash and size of existing dataset entry {ds.first().pk}")
        # If we made a new file or revision, create new dataset entry and file records
        else:
            # Create dataset entry (make default)
            new_ds = Dataset.objects.create(
                name=file_name,
                hash=file_hash,
                file_size=file_size,
                json={'aggregate_hash': new_hash},
                revision=revision,
                collection=collection,
                default_dataset=True,
                dataset_type=dataset_type,
                data_format=dataset_format,
                created_by=alyx_user,
                version=version,
            )
            # Validate dataset
            new_ds.full_clean()
            new_ds.save()
            # Make previous default dataset not default anymore (if there was one)
            if ds.count() == 1:
                _ = ds.update(default_dataset=False)
            # Change name on disk to include dataset id
            new_out_file = out_file.rename(alfiles.add_uuid_string(out_file, new_ds.pk))
            assert new_out_file.exists(), f"Failed to save renamed file {new_out_file}"
            logger.info(f"...Renamed file to {new_out_file}")
            # Create one file record per repository
            for repo in [aws_repo, fi_repo]:
                record = {
                    'dataset': new_ds,
                    'data_repository': repo,
                    'relative_path': str(out_file.relative_to(output_path)),
                    'exists': False if repo.name.startswith('aws') else True
                }
                try:
                    _ = FileRecord.objects.get_or_create(**record)
                except BaseException as e:
                    logger.error(f'...ERROR: Failed to create file record on {repo.name}: {e}')
                    status[f'{session.id}'] = f'ERROR: Failed to create file record on {repo.name}: {e}'
                    continue

            logger.info(f"...Created new dataset entry {new_ds.pk} and file records")

    except Exception as e:
        logger.error(f"...Error for subject {sub.nickname}: {e}")
        status_agg[f'{sub.id}'] = f'ERROR: {e}'
        continue

# Save status to file
status_agg = pd.DataFrame.from_dict(status_agg, orient='index', columns=['status'])
status_agg.insert(0, 'subject_id', status_agg.index)
status_agg.reset_index(drop=True, inplace=True)
status_agg.to_csv(root_path.joinpath('subjects_trials_status.csv'))

if not dry:
    # Sync whole collection folder to AWS (for now)
    src_dir = str(output_path.joinpath(collection))
    dst_dir = f's3://ibl-brain-wide-map-private/aggregates/{collection}'
    cmd = ['aws', 's3', 'sync', src_dir, dst_dir, '--delete', '--profile', 'ibladmin', '--no-progress']
    logger.info(f"Syncing {src_dir} to AWS: " + " ".join(cmd))
    t0 = time.time()
    process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
    with process.stdout:
        log_subprocess_output(process.stdout, logger.info)
    assert process.wait() == 0
    logger.debug(f'Session sync took {(time.time() - t0)} seconds')
    # Assume that everyting that existed in that folder on FI was synced and set file records to exist
    fi_frs = FileRecord.objects.filter(data_repository=fi_repo, relative_path__startswith=collection, exists=True)
    aws_frs = FileRecord.objects.filter(data_repository=aws_repo, dataset__in=fi_frs.values_list('dataset', flat=True))
    logger.info(f"Setting {aws_frs.count()} AWS file records to exists=True")
    aws_frs.update(exists=True)
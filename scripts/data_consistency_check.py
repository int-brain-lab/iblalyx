import logging
from data.models import Dataset, FileRecord, DataRepository
from one.alf.path import add_uuid_string
from iblutil.io import hashfile
import datetime
import boto3
from pathlib import Path
import iblutil.util
import pandas as pd
import numpy as np
from django.core.management import BaseCommand
import time
from dateutil.relativedelta import relativedelta as rd

logger = iblutil.util.setup_logger(__name__)
SAVE_PATH = Path.home().joinpath('ibl_logs', 'data_consistency')
SAVE_PATH.mkdir(parents=True, exist_ok=True)

log_file = logging.FileHandler(SAVE_PATH.joinpath('data_consistency.log'))
logger.addHandler(log_file)

aws_repo = DataRepository.objects.filter(name__startswith='aws').first()
session = boto3.Session()
s3 = boto3.resource('s3',
                    region_name=aws_repo.json['region_name'],
                    aws_access_key_id=aws_repo.json['Access key ID'],
                    aws_secret_access_key=aws_repo.json['Secret access key'])
bucket = s3.Bucket(name='ibl-brain-wide-map-private')

# TODO remove aggregates

def format_seconds(seconds):
    """Represent seconds in either minutes, seconds or hours depending on order of magnitude"""
    intervals = ('days', 'hours', 'minutes', 'seconds')
    x = rd(seconds=seconds)
    return ' '.join('{:.0f} {}'.format(getattr(x, k), k) for k in intervals if getattr(x, k))


class Command(BaseCommand):

    help = "Run data consistency check on datasets"
    md5 = None
    limit = None

    def add_arguments(self, parser):
        parser.add_argument('--md5', default=None, type=int, # TODO fix
                            help='Max file size to check md5 (in bytes)')
        parser.add_argument('--limit', default=500_000, type=int,
                            help='Max number of datasets to process')
        parser.add_argument('--verbosity', default=1, type=int,
                            help='Logger verbosity level 0=warning, 1=info, 2=debug')
        parser.add_argument('--dryrun', action='store_true',
                            help='Displays the operations that would be performed using the '
                                 'specified command without actually running them.')


    def handle(self, *_, **options):
        verbosity = options.pop('verbosity')
        if verbosity < 1:
            logger.setLevel(logging.WARNING)
        elif verbosity == 1:
            logger.setLevel(logging.INFO)
        elif verbosity > 1:
            logger.setLevel(logging.DEBUG)
        logger.propagate = False

        dry = options.pop('dryrun')
        t0 = time.time()
        # List of datasets to check
        datasets = self.get_datasets(**options)
        # Perform the check on the datasets
        self.check(datasets, dry=dry)
        logger.info('Entire sync and update took ' + format_seconds(time.time() - t0))

    def check(self, datasets, dry=True):

        to_investigate = []
        processed = []
        ndatasets = datasets.count()
        for i, dset in enumerate(datasets):
            if np.mod(i, 1000) == 0:
                print(f'{i}/{ndatasets} datasets processed')
            logger.info(f'Checking dataset {str(dset.id)}')

            sess_path = str(dset.session).split(' ')[1]
            lab = dset.session.lab.name
            collection = dset.collection

            if dset.revision:
                revision = f'#{dset.revision.name}#'
                if collection:
                    expected_relative_path = f'{sess_path}/{collection}/{revision}/{dset.name}'
                else:
                    expected_relative_path = f'{sess_path}/{revision}/{dset.name}'
            else:
                if collection:
                    expected_relative_path = f'{sess_path}/{collection}/{dset.name}'
                else:
                    expected_relative_path = f'{sess_path}/{dset.name}'

            expected_local_path = add_uuid_string(f'{lab}/Subjects/{expected_relative_path}', dset.id)

            frs = dset.file_records.all()
            FI_fr = [fr for fr in frs if 'flatiron' in fr.data_repository.name]
            AWS_fr = [fr for fr in frs if 'aws' in fr.data_repository.name]

            # Check the flatiron dataset
            # There should just be one FI file record
            if len(FI_fr) == 1:
                FI_fr = FI_fr[0]
                expected_FI_path = Path('/mnt/ibl').joinpath(expected_local_path)
                # Check if the dataset exists on flatiron server
                FI_exists = expected_FI_path.exists()
                FI_flag = FI_fr.exists
                if not FI_exists and FI_flag:
                    err_msg = 'FLATIRON: exists_flag=TRUE but file not found on disk'
                    logger.error(f'{str(dset.id)} - {err_msg}')
                    # Dataset does not exist but the flatiron file record flag is set to True
                    to_investigate.append(pd.DataFrame.from_dict([{'dataset_id': str(dset.id), 'problem': err_msg}]))
                elif not FI_exists and not FI_flag:
                    # If the dataset does not exist add to the list of datasets to look into
                    err_msg = 'FLATIRON: exists_flag=FALSE and file not found on disk'
                    logger.error(f'{str(dset.id)} - {err_msg}')
                    to_investigate.append(pd.DataFrame.from_dict([{'dataset_id': str(dset.id), 'problem': err_msg}]))
                else:
                    # If the dataset exists and the file record flag is set to False update the flag to True
                    if not FI_flag:
                        logger.warning(f'{str(dset.id)} - FLATIRON: exists_flag=FALSE but file found on disk - updating flag to TRUE')
                        if not dry:
                            FI_fr.exists = True
                            FI_fr.save()

                    # Check that the relative path is okay
                    if FI_fr.relative_path != expected_relative_path:
                        logger.warning(
                            f'{str(dset.id)} - FLATIRON: incorrect file record relative path - updating from {FI_fr.relative_path} to {expected_relative_path}')
                        if not dry:
                            FI_fr.relative_path = expected_relative_path
                            FI_fr.save()

                    # Check that the file size matches
                    fs = expected_FI_path.stat().st_size
                    if fs != dset.file_size:
                        logger.warning(f'{str(dset.id)} - FLATIRON: file size mismatch - updating from {dset.file_size} to {fs}')
                        if not dry:
                            dset.file_size = fs
                            dset.save()

                    # Only check md5 for files smaller than specified size (default 3GB)
                    if fs / (1024 ** 3) < 3:
                        # Check that the hash matches
                        hs = hashfile.md5(expected_FI_path)
                        if hs != dset.hash:
                            logger.warning(f'{str(dset.id)} - FLATIRON: md5 hash mismatch - updating from {dset.hash} to {hs}')
                            if not dry:
                                dset.hash = hs
                                dset.save()

                    # Get the last time the file was updated on the flatiron server
                    FI_last_updated = datetime.datetime.fromtimestamp(expected_FI_path.stat().st_mtime,
                                                                      tz=datetime.timezone.utc)

                    # For the dataset that are good on flatiron we want to check the dataset on aws
                    if len(AWS_fr) == 1:
                        AWS_fr = AWS_fr[0]
                        # Check if the dataset exists on aws bucket
                        expected_AWS_path = f'data/{str(expected_local_path)}'

                        # Check that the relative path is okay
                        if AWS_fr.relative_path != expected_relative_path:
                            logger.warning(
                                f'{str(dset.id)} - AWS: incorrect file record relative path - updating from {AWS_fr.relative_path} to {expected_relative_path}')
                            if not dry:
                                AWS_fr.relative_path = expected_relative_path
                                AWS_fr.save()

                        AWS_exists = next(iter(bucket.objects.filter(Prefix=expected_AWS_path)), False)
                        AWS_flag = AWS_fr.exists

                        # If there is no dataset on the s3 bucket we want to update the autodatetime of the dataset to ensure the sync goes through
                        if not AWS_exists:
                            logger.warning(f'{str(dset.id)} - AWS: file record found but file does not exist on s3 - updating dataset autodatetime to force resync')
                            if not dry:
                                update_time = datetime.datetime.now(datetime.timezone.utc)
                                dset.auto_datetime = update_time.isoformat()
                                dset.save()
                                # If the aws file record flag is set to True we want to set it to False so it is detected for resync
                                if AWS_flag:
                                    AWS_fr.exists = False
                                    AWS_fr.save()
                        # If there is a dataset on the s3 bucket we want to ensure that it is the latest one
                        else:
                            # We need to get the time that the dataset was last modified on s3 and compare with the last modified time on flatiron
                            # (flatiron should be less recent than aws) and also check that file size matches out
                            # If the aws dataset is older than the flatiron dataset we need to update the autodatetime of the dataset to ensure it is resynced
                            fs = AWS_exists.size
                            AWS_last_updated = AWS_exists.last_modified
                            if fs != dset.file_size or AWS_last_updated < FI_last_updated:
                                logger.warning(
                                    f'{str(dset.id)} - AWS: file record found but does not match file on flatiron - updating dataset autodatetime to force resync')
                                # Update autodatetime of dataset
                                if not dry:
                                    update_time = datetime.datetime.now(datetime.timezone.utc)
                                    dset.auto_datetime = update_time.isoformat()
                                    dset.save()
                                    # Set aws exists flag to False
                                    AWS_fr.exists = False
                                    AWS_fr.save()
                            else:
                                # Only these datasets are considered fully checked and good
                                processed.append(str(dset.id))

                    # If there is no AWS file record we need to create one and update the autodatetime of the dataset
                    elif len(AWS_fr) == 0:
                        logger.warning(
                            f'{str(dset.id)} - AWS: no aws file record found - creating one and updating dataset autodatetime to force resync')
                        aws_repo = DataRepository.objects.get(name=f'aws_{lab}')
                        if not dry:
                            FileRecord.objects.create(dataset=dset,
                                                      relative_path=expected_relative_path,
                                                      data_repository=aws_repo,
                                                      exists=False)
                            update_time = datetime.datetime.now(datetime.timezone.utc)
                            dset.auto_datetime = update_time.isoformat()
                            dset.save()

                    else:
                        err_msg = 'AWS: multiple aws file records found for dataset'
                        logger.error(f'{str(dset.id)} - {err_msg}')
                        to_investigate.append(pd.DataFrame.from_dict([{'dataset_id': str(dset.id), 'problem': err_msg}]))
            elif len(FI_fr) > 1:
                err_msg = 'FLATIRON: multiple flatiron file records for dataset'
                logger.error(f'{str(dset.id)} - {err_msg}')
                to_investigate.append(pd.DataFrame.from_dict([{'dataset_id': str(dset.id), 'problem': err_msg}]))
            elif len(FI_fr) == 0:
                err_msg = 'FLATIRON: no flatiron file record for dataset'
                logger.error(f'{str(dset.id)} - {err_msg}')
                to_investigate.append(pd.DataFrame.from_dict([{'dataset_id': str(dset.id), 'problem': err_msg}]))

        self.save_datasets(processed)

        if len(to_investigate) > 0:
            to_investigate = pd.concat(to_investigate)
            self.save_datasets_to_fix(to_investigate, processed)

    @staticmethod
    def get_datasets(**options):
        limit = options.pop('limit', 5000)
        processed_datasets_path = SAVE_PATH.joinpath('processed_datasets.npy')
        if processed_datasets_path.exists():
            processed_datasets = np.load(SAVE_PATH.joinpath('processed_datasets.npy'))
        else:
            processed_datasets = []
        to_fix_datasets = SAVE_PATH.joinpath('datasets_to_fix.pqt')
        if to_fix_datasets.exists():
            to_fix_datasets = pd.read_parquet(to_fix_datasets)
            processed_datasets = np.concatenate((processed_datasets, to_fix_datasets['dataset_id'].to_numpy()))
            processed_datasets = np.unique(processed_datasets)

        unprocessed_datasets = Dataset.objects.exclude(id__in=processed_datasets)
        # Remove aggregate datasets
        unprocessed_datasets = unprocessed_datasets.exclude(session__isnull=True)
        # Get mainly alf datasets
        alf_datasets = unprocessed_datasets.filter(collection__icontains='alf')
        alf_datasets = alf_datasets[:limit-1000]

        non_alf_datasets = unprocessed_datasets.exclude(collection__icontains='alf')
        non_alf_datasets = non_alf_datasets[:limit - alf_datasets.count()]

        datasets = alf_datasets.union(non_alf_datasets, all=True)

        return datasets

    @staticmethod
    def save_datasets(datasets):
        logger.info(f'Updating {str(SAVE_PATH.joinpath("processed_datasets.npy"))} with {len(datasets)} additional datasets')
        processed_datasets_path = SAVE_PATH.joinpath('processed_datasets.npy')
        if processed_datasets_path.exists():
            processed_datasets = np.load(SAVE_PATH.joinpath('processed_datasets.npy'))
            processed_datasets = np.concatenate((processed_datasets, np.array(datasets)))
            np.save(processed_datasets_path, processed_datasets)
        else:
            np.save(processed_datasets_path, datasets)

    @staticmethod
    def save_datasets_to_fix(datasets_to_fix, good_datasets):
        # Remove any datasets that were are now good that were previously marked to fix
        to_fix_path = SAVE_PATH.joinpath('datasets_to_fix.pqt')
        if not to_fix_path.exists():
            if len(datasets_to_fix) > 0:
                datasets_to_fix.to_parquet(SAVE_PATH.joinpath('datasets_to_fix.pqt'))
        else:
            orig_to_fix = pd.read_parquet(SAVE_PATH.joinpath('datasets_to_fix.pqt'))
            orig_to_fix = pd.concat([orig_to_fix, datasets_to_fix])
            orig_to_fix = orig_to_fix[~orig_to_fix['dataset_id'].isin(good_datasets)]
            orig_to_fix = orig_to_fix.drop_duplicates(subset='dataset_id', keep='first')
            orig_to_fix = orig_to_fix.reset_index(drop=True)
            orig_to_fix.to_parquet(SAVE_PATH.joinpath('datasets_to_fix.pqt'))



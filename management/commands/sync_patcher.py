"""
Script to query datasets that need to be synced from the s3 patcher to Flatiron.
Currently expected to run on SDSC with access to /mnt/ibl Flatiron directory.

This command is currently set up to run on the Flatiron datauser account. To install, set up a link
to the alyx data app management commands folder:
>>> basedir="/home/datauser/Documents/PYTHON"
>>> ln -s "$basedir/iblalyx/management/commands/sync_patcher.py"
"$basedir/alyx/alyx/data/management/commands/sync_patcher.py"

For the full list of options
>>> python manage.py help sync_patcher

Examples
--------
The command has 2 modes: list, and sync.

List the files to be synced and deleted: no action will be performed, log level can be set to DEBUG
>>> python manage.py sync_patcher list
>>> python manage.py sync_patcher list --verbosity 2
Run in dry mode: this will still invoke AWS commands in dry mode, and no copy will occur
>>> python manage.py sync_patcher sync --dryrun
Run the synchronization
>>> python manage.py sync_patcher sync > /home/datauser/ibl_logs/sync_patcher.log 2>&1
Force the overwrite of already existing files on flatiron (Run with extreme caution!!!!!)
>>> python manage.py sync_patcher sync --force True
"""

import time
from dateutil.relativedelta import relativedelta as rd
from subprocess import Popen, PIPE
import logging
from pathlib import Path

from one.alf.path import add_uuid_string
from django.core.paginator import Paginator
from django.core.management import BaseCommand
from django.db.models import Count, Sum, Q

from data.models import DataRepository, Dataset, FileRecord
import iblutil.util

logger = iblutil.util.setup_logger(__name__)


def run_aws_command(cmd):
    logger.debug(' '.join(cmd))
    process = Popen(cmd, stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        logger.error(stderr.decode('utf-8'))
        raise RuntimeError(f'Command {" ".join(cmd)} failed with return code {process.returncode}')
    else:
        logger.debug(stdout.decode('utf-8'))


def format_seconds(seconds):
    """Represent seconds in either minutes, seconds or hours depending on order of magnitude"""
    intervals = ('days', 'hours', 'minutes', 'seconds')
    x = rd(seconds=seconds)
    return ' '.join('{:.0f} {}'.format(getattr(x, k), k) for k in intervals if getattr(x, k))


class Command(BaseCommand):
    """Update AWS with recently changed datasets that exist locally (on FlatIron)"""
    help = "Sync S3 Patcher to Flatiron"
    limit = None
    sync_times = None
    _query = None

    def add_arguments(self, parser):
        parser.add_argument('action', default='sync', choices=['list', 'sync'])
        parser.add_argument('--session', default=None, type=str, help='Restrict datasets for a specific session')
        parser.add_argument('--user', default=None, type=str, help='Restrict to datasets for a specific user')
        parser.add_argument('--batch-size', default=50_000, type=int,
                            help='Max number of datasets per batch')
        parser.add_argument('--limit', default=500_000, type=int,
                            help='Max number of datasets to process')
        parser.add_argument('--force', default=False, type=bool,
                            help='Whether to force an overwrite of an already existing dataset')
        parser.add_argument('--dryrun', action='store_true',
                            help='Displays the operations that would be performed using the '
                                 'specified command without actually running them.')


    def handle(self, *_, **options):
        verbosity = options.pop('verbosity')
        level = logging.INFO
        if verbosity < 1:
            level = logging.WARNING
        elif verbosity > 1:
            level = logging.DEBUG
         # Diagnostic information
        logger.setLevel(level)
        logger.propagate = False

        dry = options.pop('dryrun')
        force = options.pop('force')
        t0 = time.time()
        # list the files to be synced
        query_paginated = self.build_sync_query(**options)
        # perform the sync if necessary
        if options.get('action') =='sync':
            self.sync(query_paginated, force=force, dry=dry)
            logger.info('Entire sync and update took ' + format_seconds(time.time() - t0))

    @staticmethod
    def log_dataset(qs):
        # Aggregate qs per session: count and sum of filesize
        session_stats = qs.values('session').annotate(
            dataset_count=Count('id'),
            total_size=Sum('file_size')
        )
        total_size = 0
        for stat in session_stats:
            logger.info(f"Session {stat['session']} : {stat['dataset_count']} datasets, "
                        f"{stat['total_size'] / (1024 ** 3):.2f} GB total")
            total_size += stat['total_size']
        logger.info(f'{qs.count()} files are due to be processed, representing {total_size / (1024 ** 3):.2f} GB total')
        if logger.isEnabledFor(logging.DEBUG):
            for dset in qs:
                logger.debug(f'{dset.session}, {dset}')


    @staticmethod
    def build_query(flat_iron_exists=False, **options) -> Paginator:
        """Build a paginated queryset of datasets to sync"""
        # Find datasets
        batch_size = options.pop('batch_size')
        # Find all file records with a s3 patcher data repository
        frs = FileRecord.objects.filter(data_repository__name='s3_patcher')
        # Find the associated datasets
        dsets = frs.values_list('dataset', flat=True)
        # Find the flatiron file records for the specific datasets that have exists=False
        query_kwargs = {}
        if options.get('user'):
            query_kwargs['dataset__created_by__username'] = options.get('user')
        if options.get('session'):
            query_kwargs['dataset__session'] = options.get('session')
        frs = FileRecord.objects.filter(dataset__in=dsets, data_repository__name__icontains='flatiron',
                                        exists=flat_iron_exists, **query_kwargs)
        # Get the datasets that need to be synced
        qs = Dataset.objects.filter(id__in=frs.values_list('dataset', flat=True)).order_by('session__start_time')
        # Find datasets
        Command.log_dataset(qs)
        # Restrict to specific number of datasets
        limit = options.pop('limit')
        if limit:
            qs = qs[:limit]
        return Paginator(qs, batch_size)

    @staticmethod
    def build_sync_query( **options) -> Paginator:
        """Build a paginated queryset of datasets to sync"""
        logger.info('Querying for files to sync')
        return Command.build_query(flat_iron_exists=False, **options)


    @staticmethod
    def sync(paginated_query, force=False, dry=False):
        """

        Sync the paginated query of datasets from aws s3_patcher bucket to flatiron

        :param paginated_query: paginated query of datasets to sync
        :param force: whether to overwrite an existing dataset
        :param dry: doesn't execute the actual aws sync commands
        :return:
        """

        # S3 credential information
        r = DataRepository.objects.filter(name='s3_patcher').first()
        assert r
        bucket_name = r.json['bucket_name']
        if not bucket_name.startswith('s3:'):
            bucket_name = 's3://' + bucket_name

        # Ugly hack because globus_path doesn't actually contain the correct absolute path
        ROOT = '/mnt/ibl'
        PATCHER_ROOT = '/patcher'
        t0 = time.time()

        for i in paginated_query.page_range:
            data = paginated_query.get_page(i)
            current_qs = data.object_list

            for dset in current_qs:
                fr_flatiron = dset.file_records.get(dataset=dset, data_repository__name__icontains='flatiron')
                lab_path = fr_flatiron.data_repository.globus_path
                dset_path = str(add_uuid_string(fr_flatiron.relative_path, dset.id))
                dst_file = ROOT + lab_path + dset_path
                src_file = bucket_name + PATCHER_ROOT + lab_path + dset_path
                if Path(dst_file).exists() and not force:
                    logger.info(f'Destination file for {str(dset.id)} already exists and will not be overwritten, '
                                f'set force=True to overwrite')
                    continue
                # Build up the aws command
                cmd = ['aws', 's3', 'cp', src_file, dst_file, '--profile', 'ucl', '--no-progress']
                if dry:
                    cmd.append('--dryrun')
                t0 = time.time()
                logger.info(f'{dset.session}, {dset.collection}, {dset.name}, {dset.created_by.username}')
                run_aws_command(cmd=cmd)
                if not dry:
                    exists = Path(dst_file).exists()
                    # Update the file record
                    if exists:
                        logger.info(f'Updating flatiron file record for {str(dset.id)} to True')
                        fr_flatiron.exists = True
                        fr_flatiron.save()
                        # If the aws file record exists upload the file from S3 patcher to S3 directly
                        fr_aws = FileRecord.objects.filter(dataset=dset, data_repository__name__icontains='aws').first()
                        if fr_aws is not None:
                            dst_file = 's3://ibl-brain-wide-map-private/data' + lab_path + dset_path
                            run_aws_command(cmd= ['aws', 's3', 'sync', src_file, dst_file, '--profile', 'ibladmin'])
                            fr_aws.exists = True
                            fr_aws.save()
                        # this deletes the file records from the local servers and from the S3 patcher
                        frs = dset.file_records.all().exclude(
                            Q(data_repository__name__startswith='flatiron') |
                            Q(data_repository__name__startswith='aws')
                        )
                        frs.delete()
                        logger.debug(f'Deleting file records {frs.values_list("data_repository__name")}')
                        # Delete the file from S3 patcher bucket
                        run_aws_command(cmd= ['aws', 's3', 'rm', src_file, '--profile', 'ucl'])
                        # Makes sure the last modified date is updated
                        dset.save()
                    else:
                        logger.error(f'File for {str(dset.id)} was not transferred')

        logger.debug('Datasets sync took ' + format_seconds(time.time() - t0))



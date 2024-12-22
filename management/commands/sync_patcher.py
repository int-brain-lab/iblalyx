"""Script to query datasets that need to be synced from the s3 patcher to Flatiron.
Currently expected to run on SDSC with access to /mnt/ibl Flatiron directory.

This command is currently set up to run on the Flatiron datauser account. To install, set up a link
to the alyx data app management commands folder:
>>> basedir="/home/datauser/Documents/PYTHON"
>>> ln -s "$basedir/iblalyx/management/commands/sync_patcher.py"
"$basedir/alyx/alyx/data/management/commands/sync_patcher.py"
Examples
--------
Run in dry mode
>>> python manage.py sync_patcher --dryrun > /home/datauser/ibl_logs/sync_patcher.log 2>&1
Run script
>>> python manage.py sync_patcher
Force the overwrite of already existing files on flatiron (Run with extreme caution!!!!!)
>>> python manage.py sync_patcher --force True
"""

import time
from dateutil.relativedelta import relativedelta as rd
from subprocess import Popen, PIPE, STDOUT
import logging
from pathlib import Path

from one.alf.files import add_uuid_string
from django.core.paginator import Paginator
from django.core.management import BaseCommand

from data.models import DataRepository, Dataset, FileRecord

logger = logging.getLogger('data.transfers').getChild('aws')


def log_subprocess_output(pipe, log_function=logger.info, cmd=None):
    for line in iter(pipe.readline, b''):
        log_function(line.decode().strip())
    if cmd is not None:
        log_function(f'Original command: {cmd}')


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
        # TODO Check logging works from outside main Alyx package
        verbosity = options.pop('verbosity')
        if verbosity < 1:
            logger.setLevel(logging.WARNING)
        elif verbosity == 1:
            logger.setLevel(logging.INFO)
        elif verbosity > 1:
            logger.setLevel(logging.DEBUG)

        dry = options.pop('dryrun')
        force = options.pop('force')
        t0 = time.time()
        query_paginated = self.build_sync_query(**options)
        self.sync(query_paginated, force=force, dry=dry)
        logger.debug('Entire sync and update took ' + format_seconds(time.time() - t0))
        # TODO delete old files from the patcher
        query_paginated = self.build_delete_query(**options)
        self.delete(query_paginated, dry=dry)

    @staticmethod
    def build_sync_query(**options) -> Paginator:
        """Build a paginated queryset of datasets to sync"""

        # Find datasets
        batch_size = options.pop('batch_size')
        limit = options.pop('limit')

        # Find all file records with a s3 patcher data repository
        frs = FileRecord.objects.filter(data_repository__name='s3_patcher')
        # Find the associated datasets
        dsets = frs.values_list('dataset', flat=True)
        # Find the flatiron file records for the specific datasets that have exists=False
        frs = FileRecord.objects.filter(dataset__in=dsets, data_repository__name__icontains='flatiron',
                                        exists=False)
        # Get the datasets that need to be synced
        qs = Dataset.objects.filter(id__in=frs.values_list('dataset', flat=True))

        # Restrict to specific number of datasets
        if limit:
            qs = qs[:limit]

        logger.info(f'{qs.count()} files are due to be synced')

        return Paginator(qs, batch_size)

    @staticmethod
    def build_delete_query(**options) -> Paginator:
        """Build a paginated queryset of datasets to delete from the s3 patcher. These are datasets that have
        flatiron and aws file records with both set to exists=True"""

        return

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
                fr_flatiron = FileRecord.objects.get(dataset=dset, data_repository__name__icontains='flatiron')
                lab_path = fr_flatiron.data_repository.globus_path
                dset_path = str(add_uuid_string(fr_flatiron.relative_path, dset.id))
                dst_file = ROOT + lab_path + dset_path
                src_file = bucket_name + PATCHER_ROOT + lab_path + dset_path

                if Path(dst_file).exists() and not force:
                    logger.info(f'Destination file for {str(dset.id)} already exists and will not be overwritten, '
                                f'set force=True to overwrite')
                    continue

                # Build up the aws command
                cmd = ['aws', 's3', 'cp', src_file, dst_file, '--profile', 'ibladmin']
                if dry:
                    cmd.append('--dryrun')
                if logger.level > logging.DEBUG:
                    log_fcn = logger.error
                    cmd.append('--only-show-errors')  # Suppress verbose output
                else:
                    log_fcn = logger.debug
                    cmd.append('--no-progress')  # Suppress progress info, estimated time, etc.
                logger.debug(' '.join(cmd))
                t0 = time.time()
                process = Popen(cmd, stdout=PIPE, stderr=STDOUT)
                return_code = process.wait()
                if return_code != 0:
                    with process.stderr:
                        log_subprocess_output(process.stderr, logger.error, cmd=cmd)
                    raise RuntimeError(f'Command {cmd} failed with return code {return_code}')
                else:
                    with process.stdout:
                        log_subprocess_output(process.stdout, logger.info, cmd=cmd)

                if not dry:
                    exists = Path(dst_file).exists()
                    # Update the file record
                    if exists:
                        logger.info(f'Updating flatiron file record for {str(dset.id)} to True')
                        fr_flatiron.exists = True
                        fr_flatiron.save()
                        # If the aws file record exists set it to False so the sync is re-initiated
                        fr_aws = FileRecord.objects.filter(dataset=dset, data_repository__name__icontains='aws').first()
                        if fr_aws is not None:
                            fr_aws.exists = False
                            fr_aws.save()
                        # TODO should we delete any local server file records
                    else:
                        logger.error(f'File for {str(dset.id)} was not transferred')

        logger.debug('Datasets sync took ' + format_seconds(time.time() - t0))

    @staticmethod
    def delete(paginated_query, force=False, dry=False):
        return

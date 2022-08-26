"""Script to query datasets with missing hashes or file sizes and update them based on local files.
Currently expected to run on SDSC with access to /mnt/ibl Flatiron directory.
"""
import logging
import datetime

from tqdm import tqdm
from iblutil.io import hashfile
from one.alf.files import add_uuid_string
from django.db.models import Prefetch, Q
from django.core.management import BaseCommand

from data.models import Dataset, FileRecord

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('data.integrity')
ROOT = '/mnt/ibl'  # This should be in the globus_path but isn't


class Command(BaseCommand):
    """Update AWS with recently changed datasets that exist locally (on FlatIron)"""
    help = "Update missing dataset hashes and sizes"
    _query = None
    file_records = None

    def add_arguments(self, parser):
        parser.add_argument('--limit', default=500_000, type=int,
                            help='Max number of datasets to process')
        parser.add_argument('-H', '--hostname', type=str, default='ibl.flatironinstitute.org')
        parser.add_argument('--dryrun', action='store_true',
                            help='Displays the operations that would be performed using the '
                            'specified command without actually running them.')

    def handle(self, *_, **options):
        verbosity = options.pop('verbosity')
        if verbosity < 1:
            logger.setLevel(logging.WARNING)
        elif verbosity > 1:
            logger.setLevel(logging.DEBUG)
        # Query all file records that exist on this host
        self.file_records =\
            (FileRecord
             .objects
             .select_related('data_repository')
             .filter(exists=True, data_repository__hostname=options['hostname']))

        self.update_empty(**options)
        self.update_zero(**options)

    def update_empty(self, **options):
        # Find all datasets with file_size or hash is None
        dsets = (Dataset
                 .objects
                 .filter(Q(file_size__isnull=True) | Q(hash__isnull=True), file_records__in=self.file_records)
                 .limit(options['limit']))
        dsets = dsets[:options['limit']]
        dsets = dsets.prefetch_related(Prefetch(
            'file_records',
            queryset=self.file_records,
            to_attr='flatiron_record'
        ))
        logger.info(f'{dsets.count():,d} datasets found with empty file_size or hash field')

        logger.info('Updating datasets...')
        for dset in tqdm(dsets):
            updated = False
            now = datetime.datetime.utcnow().isoformat()
            rec, = dset.flatiron_record
            path = add_uuid_string(ROOT + rec.data_repository.globus_path + rec.relative_path, dset.pk)
            try:
                if dset.file_size is None:
                    logger.debug('Updating file_size field')
                    dset.file_size = path.stat().st_size
                    assert dset.file_size is not None
                    self.update_json(dset, {'file_size_updated': now})
                    updated = True
                if dset.hash is None:
                    logger.debug('Updating hash field')
                    dset.hash = hashfile.md5(path)
                    self.update_json(dset, {'hash_updated': now})
                    updated = True
            except FileNotFoundError:
                logger.error('File not found: %s', path)
                rec.exists = False
                if not options['dry']:
                    rec.save()
            if updated:
                if options['dry']:
                    logger.info('Dataset %s file_size: %f; hash: %s', dset.pk, dset.file_size, dset.hash)
                else:
                    dset.save()

    def update_zero(self, **options):
        # Find all datasets with file_size == 0.0
        dsets = (Dataset
                 .objects
                 .filter(file_size=0., json__confirmed_empty__isnull=True, file_records__in=self.file_records))
        dsets = dsets[:options['limit']]
        dsets = dsets.prefetch_related(Prefetch(
            'file_records',
            queryset=self.file_records,
            to_attr='flatiron_record'
        ))
        logger.info(f'{dsets.count():,d} datasets found with zero file_size')

        logger.info('Updating datasets...')
        for dset in tqdm(dsets):
            updated = False
            now = datetime.datetime.utcnow().isoformat()
            rec, = dset.flatiron_record
            path = add_uuid_string(ROOT + rec.data_repository.globus_path + rec.relative_path, dset.pk)
            try:
                logger.debug('Updating file_size field')
                dset.file_size = path.stat().st_size
                assert dset.file_size is not None
                self.update_json(dset, {'confirmed_empty' if dset.file_size == 0 else 'file_size_updated': now})
                updated = True
            except FileNotFoundError:
                logger.error('File not found: %s', path)
                rec.exists = False
                if not options['dry']:
                    rec.save()
            if updated:
                if options['dry']:
                    logger.info('Dataset %s file_size: %f', dset.pk, dset.file_size)
                else:
                    dset.save()

    @staticmethod
    def update_json(record, data):
        current_data = record.json or {}
        current_data.update(data)
        record.json = current_data

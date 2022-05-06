"""Script to query recently-updated datasets and sync those specific sessions with AWS.
Currently expected to run on SDSC with access to /mnt/ibl Flatiron directory.
"""
import time
import datetime
from dateutil.relativedelta import relativedelta as rd
from subprocess import Popen, PIPE, STDOUT
import logging
import uuid

from one.alf.files import folder_parts, get_session_path, get_alf_path, add_uuid_string
import pandas as pd
from django.db.models import Q, OuterRef
from django.core.paginator import Paginator
from django.core.management import BaseCommand

from data.models import DataRepository, Dataset, FileRecord

logger = logging.getLogger('data.transfers').getChild('aws')


def log_subprocess_output(pipe, log_function=logger.info):
    for line in iter(pipe.readline, b''):
        log_function(line.decode().strip())


def format_seconds(seconds):
    """Represent seconds in either minutes, seconds or hours depending on order of magnitude"""
    intervals = ('days', 'hours', 'minutes', 'seconds')
    x = rd(seconds=seconds)
    return ' '.join('{:.0f} {}'.format(getattr(x, k), k) for k in intervals if getattr(x, k))


class Command(BaseCommand):
    """Update AWS with recently changed datasets that exist locally (on FlatIron)"""
    help = "Update AWS S3"
    limit = None
    _query = None

    def add_arguments(self, parser):
        parser.add_argument('--batch-size', default=50_000, type=int,
                            help='Max number of datasets per batch')
        parser.add_argument('--limit', default=500_000, type=int,
                            help='Max number of datasets to process')
        parser.add_argument('-hr', '--hours', type=int,
                            help='Sync datasets modified within this many hours')
        parser.add_argument('--from-date', type=datetime.datetime.fromisoformat,
                            help='Sync datasets added/modified after this date')
        parser.add_argument('--session', action='extend', nargs='+', type=uuid.UUID,
                            help='A session uuid to sync')
        parser.add_argument('-d', '--dataset', action='extend', nargs='+', type=uuid.UUID,
                            help='Dataset uuid to sync')
        parser.add_argument('-H', '--hostname', type=str, default='ibl.flatironinstitute.org')
        parser.add_argument('--dryrun', action='store_true',
                            help='Displays the operations that would be performed using the '
                            'specified command without actually running them.')

    def handle(self, *_, **options):
        # TODO Check logging works from outside main Alyx package
        verbosity = options.pop('verbosity')
        if verbosity < 1:
            logger.setLevel(logging.WARNING)
        elif verbosity > 1:
            logger.setLevel(logging.DEBUG)
        required = ('hours', 'from_date', 'session', 'dataset')
        assert any(map(options.get, required)), \
            'At least one of the following options must be passed: "%s"' % '", "'.join(required)
        dry = options.pop('dryrun')
        t0 = time.time()
        query_paginated = self.build_query(**options)
        self.sync(query_paginated, dry=dry)
        logger.debug('Entire sync and update took ' + format_seconds(time.time() - t0))

    @staticmethod
    def build_query(**options) -> Paginator:
        """Build a paginated queryset of file records to sync"""
        query = Q()
        batch_size = options.pop('batch_size')
        hostname = options.pop('hostname')
        limit = options.pop('limit')
        for k, v in options.items():
            if not v:
                continue
            if k == 'session':
                q = Q(dataset__session=v[0]) if len(v) == 1 else Q(dataset__session__in=v)
                query.add(q, Q.OR)
            elif k == 'dataset':
                q = Q(dataset__pk=v[0]) if len(v) == 1 else Q(dataset__pk__in=v)
                query.add(q, Q.OR)
            elif k == 'hours':
                nuo = datetime.datetime.now() - datetime.timedelta(hours=v)
                query.add(Q(dataset__auto_datetime__gt=nuo), Q.OR)
            elif k == 'from_date':
                query.add(Q(dataset__auto_datetime__gt=v), Q.OR)
            else:
                raise ValueError(f'Unknown kwarg "{k}"')
        # relevant fields to select
        fields = (
            'dataset__id', 'dataset__session', 'dataset__auto_datetime',
            'relative_path', 'data_repository__globus_path')
        qs = FileRecord.objects.filter(query, exists=True, data_repository__hostname=hostname)
        # Exclude file records already on AWS
        on_aws = FileRecord.objects.filter(
            dataset__in=OuterRef('dataset'), exists=True, data_repository__name__startswith='aws')
        qs = qs.exclude(dataset__in=on_aws.values_list('dataset', flat=True).distinct())
        # TODO deal with file_records that are on AWS but not FlatIron
        qs = qs.order_by('dataset__auto_datetime').select_related().values(*fields)
        if limit:
            qs = qs[:limit]
        logger.debug(qs.query)
        # logger.debug(f'{qs.count():,} file records to update')
        return Paginator(qs, batch_size)

    @staticmethod
    def sync(paginated_query, dry=False):

        # S3 credential information
        r = DataRepository.objects.filter(name__startswith='aws').first()
        assert r
        bucket_name = r.json['bucket_name']
        if not bucket_name.startswith('s3:'):
            bucket_name = 's3://' + bucket_name

        # Ugly hack because globus_path doesn't actually contain the correct absolute path
        ROOT = '/mnt/ibl'  # This should be in the globus_path but isn't
        counts = {'total': 0, 'added': 0, 'modified': 0, 'sessions': 0}
        for i in paginated_query.page_range:
            data = paginated_query.get_page(i)
            current_qs = data.object_list
            df = pd.DataFrame.from_records(current_qs)
            if df.empty:
                logger.debug('No file records to process')
                continue
            logger.info(f'Processing {len(df)} records ({i + 1}/{paginated_query.num_pages})')
            df['file_path'] = df.pop('data_repository__globus_path').str.cat(df.pop('relative_path'))
            fields_map = {
                'dataset__session': 'eid',
                'dataset__id': 'id',
                'dataset__auto_datetime': 'modified'}
            df = df.rename(fields_map, axis=1).set_index('eid')
            counts['total'] += len(df)
            # Sync is done and the session level
            for eid, rec in df.groupby('eid', axis=0):
                logger.info(f'Updating session {eid}')
                counts['sessions'] += 1
                session_path = next(map(get_session_path, rec['file_path'].values))
                src_dir = ROOT + session_path.as_posix()
                dst_dir = bucket_name.strip('/') + '/' + get_alf_path(src_dir)
                cmd = ['aws', 's3', 'sync', src_dir, dst_dir, '--delete', '--profile', 'ibladmin']
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
                with process.stdout:
                    log_subprocess_output(process.stdout, log_fcn)
                assert process.wait() == 0
                logger.debug('Session sync took ' + format_seconds(time.time() - t0))

                # Update file records
                lab, *_ = folder_parts(session_path)
                repo = f'aws_{lab}'
                for _, row in rec.iterrows():
                    record = {
                        'dataset': Dataset.objects.get(id=row['id']),
                        'data_repository': DataRepository.objects.get(name=repo),
                        'relative_path': row['file_path'].replace(f'{lab}/Subjects', '').strip('/')
                    }
                    # Check the real file path - WITH uuid in filename - exists
                    exists = add_uuid_string(ROOT + row['file_path'], row['id']).exists()
                    if dry:
                        try:
                            fr = FileRecord.objects.get(**record)
                            if fr.exists != exists:
                                counts['modified'] += 1
                                logger.info(f'(dryrun) MODIFIED: {fr.relative_path}; EXISTS = {exists}')
                        except FileRecord.DoesNotExist:
                            counts['added'] += 1
                            logger.info('(dryrun) ADDED: ' + record['relative_path'])
                    else:
                        fr, is_new = FileRecord.objects.get_or_create(**record)
                        if is_new:
                            counts['added'] += 1
                            logger.info(f'ADDED: {fr.relative_path}')
                        elif fr.exists != exists:
                            counts['modified'] += 1
                            logger.info(f'MODIFIED: {fr.relative_path}; EXISTS = {exists}')
                            fr.exists = exists
                        fr.full_clean()
                        fr.save()
        logger.info('{total:,} files over {sessions:,} sessions sync\'d; '
                    '{added:,} records added, {modified:,} modified'.format(**counts))

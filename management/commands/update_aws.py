"""Script to query recently-updated datasets and sync those specific sessions with AWS.
Currently expected to run on SDSC with access to /mnt/ibl Flatiron directory.
"""
import os
import time
import datetime
from dateutil.relativedelta import relativedelta as rd
from subprocess import Popen, PIPE, STDOUT
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import uuid
from pathlib import Path

from one.alf.path import folder_parts, get_session_path, get_alf_path, add_uuid_string
import pandas as pd
from django.db import transaction
from django.db.models import Q, OuterRef, Exists
from django.core.management import BaseCommand, CommandError

from data.models import DataRepository, FileRecord

logger = logging.getLogger('data.transfers').getChild('aws')
sync_times_file = Path.home().joinpath('Documents', '.aws_sync.csv')


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
    sync_times = None
    _query = None
    # Ugly hack because globus_path doesn't actually contain the correct absolute path
    ROOT = '/mnt/ibl'  # This should be in the globus_path but isn't
    AWS_PROFILE = 'ibladmin'
    # The options build_query understands. Everything else in the options dict belongs to
    # Django's own BaseCommand (verbosity, skip_checks, ...) and must not be passed through.
    QUERY_OPTIONS = (
        'hours', 'from_date', 'since_last', 'session', 'dataset', 'hostname', 'limit', 'force')

    def add_arguments(self, parser):
        parser.add_argument('--batch-size', default=50_000, type=int,
                            help='Number of datasets to fetch from the database at a time')
        parser.add_argument('--limit', default=500_000, type=int,
                            help='Max number of datasets to process')
        parser.add_argument('-j', '--jobs', default=4, type=int,
                            help='Number of concurrent AWS sync processes')
        parser.add_argument('-hr', '--hours', type=int,
                            help='Sync datasets modified within this many hours')
        parser.add_argument('--from-date', type=datetime.datetime.fromisoformat,
                            help='Sync datasets added/modified after this date')
        parser.add_argument('--since-last', action='store_true',
                            help='Sync datasets added/modified since the last sync')
        parser.add_argument('--session', action='extend', nargs='+', type=uuid.UUID,
                            help='A session uuid to sync')
        parser.add_argument('-d', '--dataset', action='extend', nargs='+', type=uuid.UUID,
                            help='Dataset uuid to sync')
        parser.add_argument('-H', '--hostname', type=str, default='ibl.flatironinstitute.org')
        parser.add_argument('--dryrun', action='store_true',
                            help='Displays the operations that would be performed using the '
                            'specified command without actually running them.')
        parser.add_argument('-f', '--force', action='store_true',
                            help='Sync even if file records indicate files are already on AWS.')

    def handle(self, *_, **options):
        # TODO Check logging works from outside main Alyx package
        verbosity = options.pop('verbosity')
        if verbosity < 1:
            logger.setLevel(logging.WARNING)
        elif verbosity == 1:
            logger.setLevel(logging.INFO)
        elif verbosity > 1:
            logger.setLevel(logging.DEBUG)
        required = ('hours', 'from_date', 'session', 'dataset', 'since_last')
        if not any(list(map(options.get, required))):
            options['since_last'] = True
        dry = options.pop('dryrun')
        src_host = options.get('hostname')
        save_sync_times = options.get('since_last', False)
        t0 = time.time()
        # Pass through only the query options; the rest of the dict is Django's
        query = self.build_query(**{k: options[k] for k in self.QUERY_OPTIONS if k in options})
        self.sync(
            query, dry=dry, save_sync_times=save_sync_times, source_hostname=src_host,
            batch_size=options.get('batch_size') or 50_000, jobs=options.get('jobs') or 1)
        logger.debug('Entire sync and update took ' + format_seconds(time.time() - t0))

    @staticmethod
    def last_sync(filepath=None):
        """Load sync times history"""
        sync_times = Path(filepath or sync_times_file)
        columns = ['start', 'end']
        if sync_times.exists():
            syncs = pd.read_csv(sync_times, parse_dates=columns)
        else:
            syncs = pd.DataFrame(columns=columns)
        # Force naive, nanosecond resolution: read_csv infers second or microsecond units from
        # the values it finds (pandas >= 3), and assigning a full-precision Timestamp into such a
        # column raises rather than rounding, which used to break the end-time update below.
        for column in columns:
            values = pd.to_datetime(syncs[column], errors='coerce', utc=True)
            if getattr(values.dtype, 'tz', None) is not None:
                values = values.dt.tz_localize(None)  # times are written out naive
            syncs[column] = values.astype('datetime64[ns]')
        return syncs

    @staticmethod
    def last_sync_time():
        """The start time of the last completed sync, or two weeks ago if there isn't one"""
        syncs = Command.last_sync()
        completed = syncs.loc[~syncs.end.isna(), 'start'] if not syncs.empty else []
        if len(completed) == 0:
            last_sync = pd.Timestamp.now() - pd.Timedelta(weeks=2)
        else:
            last_sync = completed.iloc[-1]
        return last_sync.floor(freq='min')

    @staticmethod
    def build_query(hours=None, from_date=None, since_last=False, session=None, dataset=None,
                    hostname=None, limit=None, force=False):
        """Build a queryset of file records to sync, ordered oldest first.

        The queryset is streamed with a server-side cursor by `sync`, rather than paginated:
        LIMIT/OFFSET over this ordering used to both re-run the (expensive) query per page and
        silently skip records, because writing a file record touches its dataset's
        auto_datetime, which is the column being ordered on.
        """
        query = Q()
        if hours:
            nuo = datetime.datetime.now() - datetime.timedelta(hours=hours)
            query.add(Q(dataset__auto_datetime__gt=nuo), Q.OR)
        if from_date:
            query.add(Q(dataset__auto_datetime__gt=from_date), Q.OR)
        if since_last:
            query.add(Q(dataset__auto_datetime__gt=Command.last_sync_time()), Q.OR)
        if session:
            query.add(Q(dataset__session__in=session), Q.OR)
            force = True  # force sync if specific sessions requested
        if dataset:
            query.add(Q(dataset__pk__in=dataset), Q.OR)
            force = True  # force sync if specific datasets requested
        if not query:
            raise ValueError('No dataset selection criteria provided')
        # relevant fields to select
        fields = (
            'dataset__id', 'dataset__session', 'dataset__collection', 'dataset__auto_datetime',
            'relative_path', 'data_repository__globus_path')
        # There are only a few dozen repositories, so resolve them to primary keys here rather
        # than making every row of the (much larger) file record scan join and match on hostname
        source_repos = list(
            DataRepository.objects.filter(hostname=hostname).values_list('pk', flat=True))
        qs = FileRecord.objects.filter(query, exists=True, data_repository_id__in=source_repos)
        if not force:
            # Exclude datasets already on AWS. A correlated NOT EXISTS lets Postgres stop at the
            # first matching row per dataset; the previous `exclude(dataset__in=<subquery>)` made
            # it deduplicate a correlated subquery for every row of the outer scan.
            aws_repos = list(DataRepository.objects
                             .filter(name__startswith='aws').values_list('pk', flat=True))
            on_aws = FileRecord.objects.filter(
                dataset=OuterRef('dataset'), exists=True, data_repository_id__in=aws_repos)
            qs = qs.filter(~Exists(on_aws))
        # TODO deal with file_records that are on AWS but not FlatIron
        # NB: dataset_id breaks ties so that --limit takes a deterministic set of records
        qs = qs.order_by('dataset__auto_datetime', 'dataset_id').values(*fields)
        if limit:
            qs = qs[:limit]
        logger.debug(qs.query)
        return qs

    @classmethod
    def group_records(cls, queryset, bucket_name, batch_size=50_000):
        """Group file records into the directories that will be synced.

        Sync happens per session directory (or, for datasets with no session, per aggregate
        collection), so many file records collapse onto a single `aws s3 sync` call. Grouping
        over the whole queryset rather than per page means a directory is synced exactly once,
        however its datasets happen to be distributed over the result set.

        Returns a dict of directory -> sync unit, in the order the records were read.

        NB: this holds one (dataset id, relative path) pair per selected file record in memory,
        i.e. it is bounded by --limit rather than --batch-size. Records are kept as bare tuples,
        and the repository root is kept once per unit rather than concatenated onto every path,
        to keep that as small as possible; --limit is the knob if a run ever needs to be capped.
        """
        units = {}
        for row in queryset.iterator(chunk_size=batch_size):
            globus_path = row['data_repository__globus_path']
            relative_path = row['relative_path']
            if row['dataset__session'] is None:
                # An aggregate: sync the collection folder it lives in
                collection = row['dataset__collection'] or ''
                src = (globus_path + relative_path)[:len(globus_path) + len(collection)]
                unit = units.get(src)
                if unit is None:
                    unit = units[src] = {
                        'src_dir': cls.ROOT + src,
                        'dst_dir': bucket_name.strip('/') + src,
                        'repository': 'aws_aggregates',
                        'globus_path': globus_path,
                        'is_session': False,
                        'records': []}
            else:
                session_path = get_session_path(globus_path + relative_path)
                src = session_path.as_posix()
                unit = units.get(src)
                if unit is None:
                    src_dir = cls.ROOT + src
                    lab, *_ = folder_parts(session_path)
                    unit = units[src] = {
                        'src_dir': src_dir,
                        'dst_dir': bucket_name.strip('/') + '/data/' + get_alf_path(src_dir),
                        'repository': f'aws_{lab}',
                        'globus_path': globus_path,
                        'is_session': True,
                        'records': []}
            unit['records'].append((row['dataset__id'], relative_path))
        return units

    @classmethod
    def run_aws_sync(cls, src_dir, dst_dir, dry=False):
        """Run `aws s3 sync` for one directory. Returns the exit code."""
        cmd = ['aws', 's3', 'sync', src_dir, dst_dir, '--delete', '--profile', cls.AWS_PROFILE]
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
        returncode = process.wait()
        logger.debug('Sync of %s took %s', src_dir, format_seconds(time.time() - t0))
        return returncode

    @classmethod
    def sync(cls, queryset, dry=False, save_sync_times=False, source_hostname=None,
             batch_size=50_000, jobs=1):
        # S3 credential information
        r = DataRepository.objects.filter(name__startswith='aws').first()
        assert r
        bucket_name = r.json['bucket_name']
        if not bucket_name.startswith('s3:'):
            bucket_name = 's3://' + bucket_name

        # Repositories are a small table; resolve them once instead of per session
        repositories = {repo.name: repo for repo in DataRepository.objects.all()}
        source_repo_ids = [
            repo.pk for repo in repositories.values() if repo.hostname == source_hostname]

        # Sync times
        sync_times = cls.last_sync()
        sync_times.loc[len(sync_times)] = [started := pd.Timestamp.now(), pd.NaT]
        if save_sync_times and not dry:
            sync_times.to_csv(sync_times_file, index=False)

        units = cls.group_records(queryset, bucket_name, batch_size=batch_size)
        if not units:
            logger.debug('No file records to process')
        counts = {'total': 0, 'added': 0, 'modified': 0, 'sessions': 0, 'missing': 0,
                  'failed': 0}
        counts['total'] = sum(len(u['records']) for u in units.values())
        counts['sessions'] = sum(1 for u in units.values() if u['is_session'])
        logger.info('Processing %s file records over %s directories',
                    f'{counts["total"]:,}', f'{len(units):,}')

        # The AWS CLI spends a long time listing the remote prefix and starting up, and each
        # directory is independent, so run several at once. Database work stays on this thread.
        exists_cache = {}
        with ThreadPoolExecutor(max_workers=max(jobs, 1)) as pool:
            futures = {
                pool.submit(cls.run_aws_sync, unit['src_dir'], unit['dst_dir'], dry=dry): unit
                for unit in units.values()}
            for future in as_completed(futures):
                unit = futures[future]
                try:
                    returncode = future.result()
                except Exception:
                    logger.exception('Failed to sync %s', unit['src_dir'])
                    counts['failed'] += 1
                    continue
                if returncode != 0:
                    # Do not touch the file records: as far as we know the files are not on AWS
                    logger.error('aws s3 sync of %s exited with %i; file records not updated',
                                 unit['src_dir'], returncode)
                    counts['failed'] += 1
                    continue
                logger.info('Updating records for %s', unit['src_dir'])
                repository = repositories.get(unit['repository'])
                if repository is None:
                    logger.error('No such data repository "%s"; file records for %s not updated',
                                 unit['repository'], unit['src_dir'])
                    counts['failed'] += 1
                    continue
                cls.update_records(
                    unit['records'], repository, globus_path=unit['globus_path'], dry=dry,
                    counts=counts, source_repository_ids=source_repo_ids,
                    exists_cache=exists_cache)

        logger.info('{total:,} files over {sessions:,} sessions sync\'d; '
                    '{added:,} records added, {modified:,} modified, '
                    '{missing:,} missing'.format(**counts))
        if counts['failed']:
            # Leave the end time unset so that the next --since-last run covers this window
            # again, rather than stepping over the directories that failed
            raise CommandError(f'{counts["failed"]} directorie(s) failed to sync')
        if save_sync_times and not dry:  # set end time
            sync_times = cls.last_sync()
            sync_times.loc[sync_times.start == started, 'end'] = pd.Timestamp.now()
            sync_times.to_csv(sync_times_file, index=False)

    @classmethod
    def exists_on_disk(cls, file_path, pk, cache=None):
        """Check the real file path - WITH uuid in filename - exists.

        Directory listings are cached: a session's file records used to cost one stat each on a
        network mount, where one listing per folder answers for all of them.
        """
        path = add_uuid_string(Path(cls.ROOT + file_path), pk)
        if cache is None:
            return path.is_file()
        if (listing := cache.get(path.parent)) is None:
            try:
                # is_file() reads the type from the directory entry where the OS provides it,
                # and only stats symlinks, so this stays one syscall for most folders
                listing = {entry.name for entry in os.scandir(path.parent) if entry.is_file()}
            except OSError:
                listing = set()
            cache[path.parent] = listing
        return path.name in listing

    @classmethod
    def update_records(cls, records, destination_repository, globus_path='', dry=False,
                       counts=None, source_repository_ids=None, exists_cache=None):
        """Update file records with new data repository information.

        `records` is a list of (dataset id, relative path) pairs all belonging to the same
        destination repository, and `globus_path` is the source repository root they are relative
        to. The whole list is resolved with a couple of queries instead of the half-dozen per
        record that get_or_create + full_clean + save cost: at half a million file records per
        run that fan-out dominated the runtime.

        NB: bulk writes do not call FileRecord.save, so the datasets are no longer re-saved for
        each of their file records. That is deliberate - it kept bumping Dataset.auto_datetime,
        which is the field the incremental query filters on, so every dataset the command touched
        re-entered the next run's window.
        """
        counts = counts if counts is not None else {'added': 0, 'modified': 0, 'missing': 0}
        if isinstance(destination_repository, str):
            destination_repository = DataRepository.objects.get(name=destination_repository)
        if not records:
            return

        # Which of these files are actually on disk?
        on_disk = {
            pk: cls.exists_on_disk(globus_path + relative_path, pk, exists_cache)
            for pk, relative_path in records}

        # One query for every file record we might already have on the destination
        existing = {
            (r['dataset_id'], r['relative_path']): r
            for r in FileRecord.objects
            .filter(data_repository=destination_repository, dataset_id__in=list(on_disk))
            .values('pk', 'dataset_id', 'relative_path', 'exists')}

        to_create, to_update, missing = [], [], []
        for pk, relative_path in records:
            exists = on_disk[pk]
            relative_path = relative_path.strip('/')
            if not exists:
                missing.append(pk)
            current = existing.get((pk, relative_path))
            if current is None:
                counts['added'] += 1
                file_record = FileRecord(
                    dataset_id=pk, data_repository=destination_repository,
                    relative_path=relative_path, exists=exists)
                # Validate relative_path against the model's own validators. The foreign keys
                # are excluded because ForeignKey.validate issues a query per key to check the
                # referenced row is there, which the database enforces on insert anyway; that
                # and full_clean's unique_together check were 3 of the ~17 queries per record.
                file_record.clean_fields(exclude=('dataset', 'data_repository'))
                to_create.append(file_record)
                logger.info(('(dryrun) ' if dry else '') + 'ADDED: ' + relative_path)
            elif current['exists'] != exists:
                counts['modified'] += 1
                to_update.append(FileRecord(pk=current['pk'], exists=exists))
                logger.info(('(dryrun) ' if dry else '')
                            + f'MODIFIED: {relative_path}; EXISTS = {exists}')
            if not exists:
                counts['missing'] += 1
                logger.warning(f'MISSING: {relative_path}; EXISTS = {exists}')

        if dry:
            return

        # One transaction per directory: the alternative is a commit (and an fsync) for each of
        # the statements below, and a half-updated directory if the run dies between them
        with transaction.atomic():
            if to_create:
                FileRecord.objects.bulk_create(to_create, batch_size=1000)
            if to_update:
                FileRecord.objects.bulk_update(to_update, ['exists'], batch_size=1000)
            # If the files don't exist on the source repository, mark them as such
            if missing:
                if not source_repository_ids:
                    raise CommandError(
                        'Source repository must be provided to mark missing files')
                FileRecord.objects.filter(
                    dataset_id__in=missing, data_repository_id__in=source_repository_ids
                ).update(exists=False)


# def sync_changed():
#     """Sync all sessions where file records on flatiron don't match those on AWS.
#
#     Note: The next version of Django has an XOR Q filter, until then, this method is too slow.
#     """
#     from django.db.models import Exists, F, Count
#     fr = FileRecord.objects.select_related('data_repository')
#     # File records on Flatiron
#     on_flatiron = fr.filter(dataset=OuterRef('pk'),
#                             exists=True,
#                             data_repository__name__startswith='flatiron').values_list('pk', flat=True)
#     # File records on AWS
#     on_aws = fr.filter(dataset=OuterRef('pk'),
#                        exists=True,
#                        data_repository__name__startswith='aws').values_list('pk', flat=True)
#     # Filter out datasets that do not exist on either repository
#     ds = Dataset.objects.alias(exists_flatiron=Exists(on_flatiron), exists_aws=Exists(on_aws))
#     on_aws = Q(exists_aws=True)
#     on_flatiron = Q(exists_flatiron=True)
#     xor_ds = ds.filter((on_aws & ~on_flatiron) | (~on_aws & on_flatiron)).distinct().values_list('pk', flat=True)  # 47416
#     # xor_ds = ds.alias(mismatch=Count(F('exists_aws')) + Count(F('exists_flatiron'))).filter(mismatch=1)
#     fr = fr.filter(exists=True, data_repository__globus_is_personal=False, dataset__in=xor_ds)
#     # This isn't going to work :(
#     on_server = (FileRecord
#                  .objects
#                  .select_related('data_repository')
#                  .filter(dataset=OuterRef('pk'), exists=True, data_repository__globus_is_personal=False)
#                  .values_list('pk', flat=True))
#     ds = Dataset.objects.select_related('file_record').alias(mismatch=Count(on_server)).filter(mismatch=1)

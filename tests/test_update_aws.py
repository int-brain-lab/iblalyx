"""Tests for the ``update_aws`` management command.

These tests pin down the observable behaviour of the command rather than the signatures of its
helpers, because this is a sensitive part of the data pipeline that gets refactored for
performance from time to time. "Observable" here means three things:

1. the ``aws s3 sync`` command lines the command shells out to,
2. the state of the ``FileRecord`` table afterwards, and
3. the sync-times CSV that ``--since-last`` reads back on the next run.

Nothing here touches a real S3 bucket or the real ``~/Documents/.aws_sync.csv``: ``Popen`` is
replaced with a recorder, ``Command.ROOT`` points at a temporary tree, and the sync-times path is
patched to live in that tree.

Run with (from the alyx checkout, with the iblalyx parent directory on ``PYTHONPATH``)::

    python manage.py test iblalyx.tests.test_update_aws
"""
import io
import uuid
import datetime
import tempfile
from pathlib import Path
from unittest import mock

import pandas as pd
from django.core.management import call_command, CommandError
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from django.db import connection
from one.alf.path import add_uuid_string

from misc.models import Lab
from subjects.models import Subject
from actions.models import Session
from data.models import DataRepository, Dataset, FileRecord

from iblalyx.management.commands import update_aws

HOSTNAME = 'ibl.flatironinstitute.org'
BUCKET = 'ibl-brain-wide-map-private'


class FakeProcess:
    """Stands in for a ``Popen`` object wrapping ``aws s3 sync``."""

    def __init__(self, output=b'', returncode=0):
        self.stdout = io.BytesIO(output)
        self.returncode = returncode

    def wait(self):
        return self.returncode


class AWSSyncTests(TestCase):
    """Base class providing an isolated filesystem, a fake AWS CLI and a small database."""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)

        # Keep the command away from the real Flatiron mount and the real sync-times file
        self._patch(mock.patch.object(update_aws.Command, 'ROOT', self.root.as_posix()))
        self.sync_times_file = self.root / '.aws_sync.csv'
        self._patch(mock.patch.object(update_aws, 'sync_times_file', self.sync_times_file))

        # Record the AWS CLI invocations instead of running them
        self.aws_calls = []
        self.returncode = 0
        self.fail_dirs = set()  # source directories whose sync should report failure
        self._patch(mock.patch.object(update_aws, 'Popen', self._popen))

        self._create_repositories()

    def _patch(self, patcher):
        patcher.start()
        self.addCleanup(patcher.stop)

    def _popen(self, cmd, **_):
        self.aws_calls.append(list(cmd))
        returncode = 1 if cmd[3] in self.fail_dirs else self.returncode
        return FakeProcess(returncode=returncode)

    # ------------------------------------------------------------------ fixtures
    def _create_repositories(self):
        """Create the Flatiron source repositories and their AWS counterparts.

        ``globus_path`` deliberately mirrors production: the main Flatiron repository is rooted at
        '/' with lab-prefixed relative paths, while aggregates live under '/aggregates/'.
        """
        self.flatiron = DataRepository.objects.create(
            name='flatiron', hostname=HOSTNAME, globus_path='/',
            globus_is_personal=False, globus_endpoint_id=uuid.uuid4())
        self.flatiron_aggregates = DataRepository.objects.create(
            name='flatiron_aggregates', hostname=HOSTNAME, globus_path='/aggregates/',
            globus_is_personal=False, globus_endpoint_id=uuid.uuid4())
        self.aws = {
            name: DataRepository.objects.create(
                name=name, hostname='s3.amazonaws.com', json={'bucket_name': BUCKET})
            for name in ('aws_aggregates', 'aws_churchlandlab', 'aws_mainenlab')}

    def _create_session(self, lab='mainenlab', nickname='SWC_042',
                        date='2020-01-01', number=1):
        lab, _ = Lab.objects.get_or_create(name=lab)
        subject, _ = Subject.objects.get_or_create(nickname=nickname, lab=lab)
        session = Session.objects.create(
            subject=subject, lab=lab, number=number,
            start_time=datetime.datetime.fromisoformat(date))
        session.alf_path = f'{lab.name}/Subjects/{nickname}/{date}/{number:03d}'
        return session

    def _create_dataset(self, session=None, collection='alf', name='_ibl_trials.table.pqt',
                        repository=None, on_disk=True, auto_datetime=None, exists=True):
        """Create a dataset with one source file record, and optionally the file on disk.

        Returns the dataset. ``auto_datetime`` is applied last because saving a file record
        cascades a save onto the dataset, which would otherwise overwrite it.
        """
        repository = repository or self.flatiron
        if session is None:  # an aggregate: relative path is rooted at the aggregates repository
            relative_path = f'{collection}/{name}'
        else:
            relative_path = f'{session.alf_path}/{collection}/{name}'
        dataset = Dataset.objects.create(name=name, session=session, collection=collection)
        FileRecord.objects.create(
            dataset=dataset, data_repository=repository,
            relative_path=relative_path, exists=exists)
        if on_disk:
            self._touch(repository, relative_path, dataset.pk)
        if auto_datetime is not None:
            Dataset.objects.filter(pk=dataset.pk).update(auto_datetime=auto_datetime)
        dataset.refresh_from_db()
        return dataset

    def _touch(self, repository, relative_path, pk):
        """Create the on-disk file, with the dataset UUID in the filename as on Flatiron."""
        path = Path(self.root.as_posix() + repository.globus_path + relative_path)
        path = add_uuid_string(path, pk)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
        return path

    def _add_aws_record(self, dataset, repository, exists=True):
        """Mark a dataset as already present on AWS."""
        source = dataset.file_records.exclude(data_repository__name__startswith='aws').first()
        return FileRecord.objects.create(
            dataset=dataset, data_repository=self.aws[repository],
            relative_path=source.relative_path, exists=exists)

    # ------------------------------------------------------------------ helpers
    def _records(self, argv):
        """Return the file records ``build_query`` selects for the given command line."""
        return list(self._build_query(argv))

    def _build_query(self, argv):
        options = self._parse(argv)
        required = ('hours', 'from_date', 'session', 'dataset', 'since_last')
        if not any(map(options.get, required)):
            options['since_last'] = True
        query_options = update_aws.Command.QUERY_OPTIONS
        return update_aws.Command.build_query(
            **{k: options[k] for k in query_options if k in options})

    @staticmethod
    def _parse(argv):
        """Parse a command line the way ``manage.py`` would, less the base Django options."""
        parser = update_aws.Command().create_parser('manage.py', 'update_aws')
        options = vars(parser.parse_args(list(argv)))
        for key in ('verbosity', 'settings', 'pythonpath', 'traceback',
                    'no_color', 'force_color', 'skip_checks'):
            options.pop(key, None)
        return options

    def _run(self, *argv):
        """Run the command end to end with the AWS CLI stubbed out.

        This mirrors ``BaseCommand.run_from_argv``, i.e. the path ``manage.py update_aws`` takes
        in the cron job, rather than ``call_command``. See ``TestCallCommand``.
        """
        argv = list(argv)
        if not any(a.startswith('-v') or a == '--verbosity' for a in argv):
            argv += ['-v', '0']
        command = update_aws.Command()
        options = vars(command.create_parser('manage.py', 'update_aws').parse_args(argv))
        args = options.pop('args', ())
        return command.execute(*args, **options)

    def _sync_calls(self):
        """The (src, dst) pairs of each recorded ``aws s3 sync`` invocation."""
        return [(cmd[3], cmd[4]) for cmd in self.aws_calls]


class TestBuildQuery(AWSSyncTests):
    """The selection of file records to sync."""

    def test_hours_selects_recently_modified(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        session = self._create_session()
        recent = self._create_dataset(
            session, name='recent.pqt', auto_datetime=now - datetime.timedelta(hours=1))
        self._create_dataset(
            session, name='stale.pqt', auto_datetime=now - datetime.timedelta(hours=48))
        records = self._records(['--hours', '24'])
        self.assertEqual([recent.pk], [r['dataset__id'] for r in records])

    def test_from_date(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        session = self._create_session()
        after = self._create_dataset(
            session, name='after.pqt', auto_datetime=now - datetime.timedelta(days=1))
        self._create_dataset(
            session, name='before.pqt', auto_datetime=now - datetime.timedelta(days=10))
        cutoff = (now - datetime.timedelta(days=5)).replace(tzinfo=None).isoformat()
        records = self._records(['--from-date', cutoff])
        self.assertEqual([after.pk], [r['dataset__id'] for r in records])

    def test_since_last_without_history_uses_two_week_window(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        session = self._create_session()
        recent = self._create_dataset(
            session, name='recent.pqt', auto_datetime=now - datetime.timedelta(days=3))
        self._create_dataset(
            session, name='old.pqt', auto_datetime=now - datetime.timedelta(days=20))
        self.assertFalse(self.sync_times_file.exists())
        records = self._records([])
        self.assertEqual([recent.pk], [r['dataset__id'] for r in records])

    def test_since_last_uses_last_completed_sync(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        # A completed sync 2 days ago and an unfinished one an hour ago: the completed one wins
        pd.DataFrame({
            'start': [now - datetime.timedelta(days=2), now - datetime.timedelta(hours=1)],
            'end': [now - datetime.timedelta(days=2), pd.NaT],
        }).to_csv(self.sync_times_file, index=False)
        session = self._create_session()
        recent = self._create_dataset(
            session, name='recent.pqt', auto_datetime=now - datetime.timedelta(hours=6))
        self._create_dataset(
            session, name='old.pqt', auto_datetime=now - datetime.timedelta(days=5))
        records = self._records([])
        self.assertEqual([recent.pk], [r['dataset__id'] for r in records])

    def test_session_selects_all_datasets_of_session(self):
        wanted = self._create_session(nickname='SWC_042')
        other = self._create_session(nickname='SWC_043')
        datasets = [self._create_dataset(wanted, name=f'a{i}.pqt') for i in range(2)]
        self._create_dataset(other, name='b.pqt')
        records = self._records(['--session', str(wanted.pk)])
        self.assertCountEqual([d.pk for d in datasets], [r['dataset__id'] for r in records])

    def test_dataset_selects_named_datasets(self):
        session = self._create_session()
        wanted = self._create_dataset(session, name='a.pqt')
        self._create_dataset(session, name='b.pqt')
        records = self._records(['--dataset', str(wanted.pk)])
        self.assertEqual([wanted.pk], [r['dataset__id'] for r in records])

    def test_excludes_datasets_already_on_aws(self):
        session = self._create_session()
        on_aws = self._create_dataset(session, name='on_aws.pqt')
        self._add_aws_record(on_aws, 'aws_mainenlab', exists=True)
        pending = self._create_dataset(session, name='pending.pqt')
        records = self._records(['--hours', '24'])
        self.assertEqual([pending.pk], [r['dataset__id'] for r in records])

    def test_aws_record_that_does_not_exist_is_not_an_exclusion(self):
        """A file record on AWS with exists=False still needs syncing."""
        session = self._create_session()
        dataset = self._create_dataset(session, name='ghost.pqt')
        self._add_aws_record(dataset, 'aws_mainenlab', exists=False)
        records = self._records(['--hours', '24'])
        self.assertEqual([dataset.pk], [r['dataset__id'] for r in records])

    def test_force_includes_datasets_already_on_aws(self):
        session = self._create_session()
        dataset = self._create_dataset(session, name='on_aws.pqt')
        self._add_aws_record(dataset, 'aws_mainenlab', exists=True)
        self.assertEqual([], self._records(['--hours', '24']))
        records = self._records(['--hours', '24', '--force'])
        self.assertEqual([dataset.pk], [r['dataset__id'] for r in records])

    def test_explicit_session_forces_sync(self):
        """--session implies --force, so an already-synced session is re-selected."""
        session = self._create_session()
        dataset = self._create_dataset(session, name='on_aws.pqt')
        self._add_aws_record(dataset, 'aws_mainenlab', exists=True)
        records = self._records(['--session', str(session.pk)])
        self.assertEqual([dataset.pk], [r['dataset__id'] for r in records])

    def test_ignores_source_records_that_do_not_exist(self):
        session = self._create_session()
        present = self._create_dataset(session, name='present.pqt')
        self._create_dataset(session, name='absent.pqt', exists=False)
        records = self._records(['--hours', '24'])
        self.assertEqual([present.pk], [r['dataset__id'] for r in records])

    def test_ignores_other_hostnames(self):
        session = self._create_session()
        elsewhere = DataRepository.objects.create(
            name='some_lab_local', hostname='lab.example.org', globus_path='/',
            globus_is_personal=True, globus_endpoint_id=uuid.uuid4())
        mine = self._create_dataset(session, name='mine.pqt')
        self._create_dataset(session, name='theirs.pqt', repository=elsewhere)
        records = self._records(['--hours', '24'])
        self.assertEqual([mine.pk], [r['dataset__id'] for r in records])

    def test_limit(self):
        session = self._create_session()
        for i in range(5):
            self._create_dataset(session, name=f'a{i}.pqt')
        self.assertEqual(2, len(self._records(['--hours', '24', '--limit', '2'])))

    def test_ordered_by_auto_datetime(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        session = self._create_session()
        expected = []
        for i in (3, 1, 2):  # created out of order
            dataset = self._create_dataset(
                session, name=f'a{i}.pqt', auto_datetime=now - datetime.timedelta(hours=i))
            expected.append((i, dataset.pk))
        expected = [pk for _, pk in sorted(expected, reverse=True)]
        records = self._records(['--hours', '24'])
        self.assertEqual(expected, [r['dataset__id'] for r in records])

    def test_selected_fields(self):
        """The fields the sync stage relies on must all be present."""
        session = self._create_session()
        self._create_dataset(session)
        record, = self._records(['--hours', '24'])
        self.assertEqual(
            {'dataset__id', 'dataset__session', 'dataset__collection',
             'dataset__auto_datetime', 'relative_path', 'data_repository__globus_path'},
            set(record))


class TestSessionSync(AWSSyncTests):
    """Syncing session datasets and updating their file records."""

    def test_syncs_session_directory_once(self):
        session = self._create_session()
        for i in range(3):
            self._create_dataset(session, name=f'a{i}.pqt')
        self._run('--hours', '24')
        expected_src = f'{self.root.as_posix()}/mainenlab/Subjects/SWC_042/2020-01-01/001'
        expected_dst = f's3://{BUCKET}/data/mainenlab/Subjects/SWC_042/2020-01-01/001'
        self.assertEqual([(expected_src, expected_dst)], self._sync_calls())

    def test_sync_command_line(self):
        session = self._create_session()
        self._create_dataset(session)
        self._run('--hours', '24')
        cmd, = self.aws_calls
        self.assertEqual(['aws', 's3', 'sync'], cmd[:3])
        self.assertIn('--delete', cmd)
        self.assertEqual(['--profile', 'ibladmin'], cmd[6:8])
        self.assertNotIn('--dryrun', cmd)

    def test_one_sync_per_session(self):
        sessions = [
            self._create_session(lab='mainenlab', nickname='SWC_042'),
            self._create_session(lab='churchlandlab', nickname='CSHL_003'),
        ]
        for session in sessions:
            self._create_dataset(session, name='a.pqt')
            self._create_dataset(session, name='b.pqt')
        self._run('--hours', '24')
        self.assertEqual(2, len(self.aws_calls))
        self.assertCountEqual(
            [f'{self.root.as_posix()}/mainenlab/Subjects/SWC_042/2020-01-01/001',
             f'{self.root.as_posix()}/churchlandlab/Subjects/CSHL_003/2020-01-01/001'],
            [src for src, _ in self._sync_calls()])

    def test_creates_aws_file_records_in_lab_repository(self):
        session = self._create_session(lab='churchlandlab', nickname='CSHL_003')
        dataset = self._create_dataset(session, name='a.pqt')
        self._run('--hours', '24')
        record = FileRecord.objects.get(
            dataset=dataset, data_repository=self.aws['aws_churchlandlab'])
        self.assertEqual(
            'churchlandlab/Subjects/CSHL_003/2020-01-01/001/alf/a.pqt', record.relative_path)

    def test_corrects_exists_on_existing_aws_record(self):
        session = self._create_session()
        dataset = self._create_dataset(session, name='a.pqt', on_disk=True)
        record = self._add_aws_record(dataset, 'aws_mainenlab', exists=False)
        self._run('--hours', '24', '--force')
        record.refresh_from_db()
        self.assertTrue(record.exists)

    def test_missing_file_marks_source_record_absent(self):
        """When the file is not on disk, the Flatiron record is corrected to exists=False."""
        session = self._create_session()
        dataset = self._create_dataset(session, name='gone.pqt', on_disk=False)
        source = dataset.file_records.get(data_repository=self.flatiron)
        self.assertTrue(source.exists)
        self._run('--hours', '24')
        source.refresh_from_db()
        self.assertFalse(source.exists)

    def test_present_file_leaves_source_record_alone(self):
        session = self._create_session()
        dataset = self._create_dataset(session, name='here.pqt', on_disk=True)
        self._run('--hours', '24')
        source = dataset.file_records.get(data_repository=self.flatiron)
        self.assertTrue(source.exists)

    def test_dryrun_makes_no_database_changes(self):
        session = self._create_session()
        dataset = self._create_dataset(session, name='a.pqt')
        before = set(FileRecord.objects.values_list('pk', flat=True))
        self._run('--hours', '24', '--dryrun')
        self.assertEqual(before, set(FileRecord.objects.values_list('pk', flat=True)))
        self.assertFalse(FileRecord.objects
                         .filter(dataset=dataset, data_repository__name__startswith='aws')
                         .exists())

    def test_dryrun_passes_flag_to_aws(self):
        session = self._create_session()
        self._create_dataset(session)
        self._run('--hours', '24', '--dryrun')
        cmd, = self.aws_calls
        self.assertIn('--dryrun', cmd)

    def test_dryrun_does_not_mark_source_absent(self):
        session = self._create_session()
        dataset = self._create_dataset(session, name='gone.pqt', on_disk=False)
        self._run('--hours', '24', '--dryrun')
        source = dataset.file_records.get(data_repository=self.flatiron)
        self.assertTrue(source.exists)

    def test_no_records_makes_no_aws_calls(self):
        self._create_session()
        self._run('--hours', '24')
        self.assertEqual([], self.aws_calls)

    def test_datasets_in_subcollections_sync_at_session_level(self):
        """Collections do not narrow the sync: the whole session directory is synced."""
        session = self._create_session()
        self._create_dataset(session, collection='alf', name='a.pqt')
        self._create_dataset(session, collection='raw_ephys_data/probe00', name='b.bin')
        self._run('--hours', '24')
        self.assertEqual(1, len(self.aws_calls))
        self.assertEqual(
            f'{self.root.as_posix()}/mainenlab/Subjects/SWC_042/2020-01-01/001',
            self._sync_calls()[0][0])


class TestAggregateSync(AWSSyncTests):
    """Syncing datasets with no session, which are synced per collection."""

    def _create_aggregate(self, lab='mainenlab', nickname='SWC_042',
                          name='_ibl_subjectTrials.table.pqt', **kwargs):
        collection = f'Subjects/{lab}/{nickname}'
        return self._create_dataset(
            session=None, collection=collection, name=name,
            repository=self.flatiron_aggregates, **kwargs)

    def test_syncs_at_collection_level(self):
        self._create_aggregate()
        self._run('--hours', '24')
        expected_src = f'{self.root.as_posix()}/aggregates/Subjects/mainenlab/SWC_042'
        expected_dst = f's3://{BUCKET}/aggregates/Subjects/mainenlab/SWC_042'
        self.assertEqual([(expected_src, expected_dst)], self._sync_calls())

    def test_one_sync_per_collection(self):
        self._create_aggregate(nickname='SWC_042', name='a.pqt')
        self._create_aggregate(nickname='SWC_042', name='b.pqt')
        self._create_aggregate(nickname='SWC_043', name='c.pqt')
        self._run('--hours', '24')
        self.assertEqual(2, len(self.aws_calls))
        self.assertCountEqual(
            [f'{self.root.as_posix()}/aggregates/Subjects/mainenlab/SWC_042',
             f'{self.root.as_posix()}/aggregates/Subjects/mainenlab/SWC_043'],
            [src for src, _ in self._sync_calls()])

    def test_records_created_in_aggregates_repository(self):
        dataset = self._create_aggregate()
        self._run('--hours', '24')
        record = FileRecord.objects.get(
            dataset=dataset, data_repository=self.aws['aws_aggregates'])
        self.assertEqual(
            'Subjects/mainenlab/SWC_042/_ibl_subjectTrials.table.pqt', record.relative_path)

    def test_missing_aggregate_marks_source_absent(self):
        dataset = self._create_aggregate(on_disk=False)
        self._run('--hours', '24')
        source = dataset.file_records.get(data_repository=self.flatiron_aggregates)
        self.assertFalse(source.exists)

    def test_sessions_and_aggregates_in_one_run(self):
        session = self._create_session()
        session_dataset = self._create_dataset(session, name='a.pqt')
        aggregate = self._create_aggregate(name='b.pqt')
        self._run('--hours', '24')
        self.assertEqual(2, len(self.aws_calls))
        self.assertTrue(FileRecord.objects.filter(
            dataset=session_dataset, data_repository=self.aws['aws_mainenlab']).exists())
        self.assertTrue(FileRecord.objects.filter(
            dataset=aggregate, data_repository=self.aws['aws_aggregates']).exists())


class TestSyncTimes(AWSSyncTests):
    """The sync-times CSV that --since-last reads."""

    def test_since_last_records_start_and_end(self):
        session = self._create_session()
        self._create_dataset(session)
        self._run()
        syncs = pd.read_csv(self.sync_times_file, parse_dates=[0, 1])
        self.assertEqual(1, len(syncs))
        self.assertFalse(syncs.iloc[0].isna().any())
        self.assertLessEqual(syncs.iloc[0]['start'], syncs.iloc[0]['end'])

    def test_since_last_appends(self):
        session = self._create_session()
        self._create_dataset(session)
        self._run()
        self._run()
        self.assertEqual(2, len(pd.read_csv(self.sync_times_file)))

    def test_explicit_window_does_not_write_sync_times(self):
        session = self._create_session()
        self._create_dataset(session)
        self._run('--hours', '24')
        self.assertFalse(self.sync_times_file.exists())

    def test_dryrun_does_not_write_sync_times(self):
        session = self._create_session()
        self._create_dataset(session)
        self._run('--dryrun')
        self.assertFalse(self.sync_times_file.exists())


class TestBatching(AWSSyncTests):
    """Behaviour must not depend on the batch size.

    Batching only kicks in above ``--batch-size`` (50,000) file records, which is why the
    defects these cover went unnoticed for so long: the nightly cron never filled a page. A
    backfill does, and ``--limit`` defaults to 500,000.
    """

    def test_session_synced_once_across_batches(self):
        """A session's datasets spread over several fetches are synced as one directory.

        Records used to be grouped per page, so each page re-ran ``aws s3 sync --delete`` over
        the same session directory, repeating the remote listing for no benefit.
        """
        session = self._create_session()
        for i in range(6):
            self._create_dataset(session, name=f'a{i}.pqt')
        self._run('--hours', '24', '--batch-size', '2')
        self.assertEqual(1, len(self.aws_calls))
        self.assertEqual({(f'{self.root.as_posix()}/mainenlab/Subjects/SWC_042/2020-01-01/001',
                           f's3://{BUCKET}/data/mainenlab/Subjects/SWC_042/2020-01-01/001')},
                         set(self._sync_calls()))

    def test_all_records_updated_across_batches(self):
        """No dataset is dropped when the run spans more than one fetch.

        Regression test. ``build_query`` orders by ``dataset__auto_datetime``, and the old
        paginator walked that ordering with LIMIT/OFFSET, re-running the query per page. Updating
        a file record cascaded a save onto its dataset, which bumped ``auto_datetime`` and moved
        the row to the end of the ordering, so the next page's OFFSET stepped over rows that had
        shifted underneath it: only 2 of these 4 datasets used to get an AWS file record.
        """
        for i in range(4):
            session = self._create_session(nickname=f'SWC_04{i}')
            self._create_dataset(session, name='a.pqt')
        self._run('--hours', '24', '--batch-size', '1')
        self.assertEqual(4, len(self.aws_calls))
        self.assertEqual(4, FileRecord.objects.filter(
            data_repository=self.aws['aws_mainenlab']).count())

    def test_single_batch_updates_every_record(self):
        """The same run at the default batch size."""
        for i in range(4):
            session = self._create_session(nickname=f'SWC_04{i}')
            self._create_dataset(session, name='a.pqt')
        self._run('--hours', '24')
        self.assertEqual(4, len(self.aws_calls))
        self.assertEqual(4, FileRecord.objects.filter(
            data_repository=self.aws['aws_mainenlab']).count())


class TestExistsFlag(AWSSyncTests):
    """The ``exists`` flag on newly created AWS file records."""

    def test_new_aws_record_exists_flag(self):
        """A new AWS record for a file that is on disk is marked as existing.

        It used to be left at the model default of False, because ``update_records`` passed the
        same dict to both halves of ``get_or_create`` and so never supplied ``exists``. Since
        ``build_query`` only excludes datasets holding an AWS record with exists=True, that meant
        the dataset was selected again by every subsequent non-forced run, indefinitely.
        """
        session = self._create_session()
        dataset = self._create_dataset(session, name='a.pqt', on_disk=True)
        self._run('--hours', '24')
        record = FileRecord.objects.get(
            dataset=dataset, data_repository=self.aws['aws_mainenlab'])
        self.assertTrue(record.exists)

    def test_new_aws_record_for_missing_file(self):
        """A file that is not on disk is recorded as not existing on AWS either."""
        session = self._create_session()
        dataset = self._create_dataset(session, name='gone.pqt', on_disk=False)
        self._run('--hours', '24')
        record = FileRecord.objects.get(
            dataset=dataset, data_repository=self.aws['aws_mainenlab'])
        self.assertFalse(record.exists)

    def test_synced_dataset_is_retired(self):
        """Follows from the above: a successful sync takes the dataset out of the work queue."""
        session = self._create_session()
        self._create_dataset(session, name='a.pqt', on_disk=True)
        self._run('--hours', '24')
        self.assertEqual([], self._records(['--hours', '24']))


class TestSourceDatasetSideEffects(AWSSyncTests):
    """Writing AWS file records must not disturb the source datasets."""

    def test_sync_does_not_bump_dataset_auto_datetime(self):
        """The field the incremental query filters on is left alone.

        ``FileRecord.save`` re-saves its dataset, so the row-by-row implementation refreshed
        ``Dataset.auto_datetime`` for every dataset it touched, putting them all back inside the
        next ``--since-last`` / ``--hours`` window. The bulk writes skip ``save``.
        """
        session = self._create_session()
        dataset = self._create_dataset(session, name='a.pqt')
        before = dataset.auto_datetime
        self._run('--hours', '24')
        dataset.refresh_from_db()
        self.assertEqual(before, dataset.auto_datetime)

    def test_source_file_record_untouched_when_file_present(self):
        session = self._create_session()
        dataset = self._create_dataset(session, name='a.pqt', on_disk=True)
        source = dataset.file_records.get(data_repository=self.flatiron)
        self._run('--hours', '24')
        source.refresh_from_db()
        self.assertTrue(source.exists)


class TestCallCommand(AWSSyncTests):
    """Invocation through ``call_command`` rather than the command line."""

    def test_call_command_works(self):
        """``call_command`` can drive the command.

        Regression test. ``build_query`` used to receive the whole options dict and raise on any
        key it did not recognise, so ``call_command``'s injected ``skip_checks=True`` (which the
        command line leaves False) tripped the check on one of Django's own options.
        """
        session = self._create_session()
        dataset = self._create_dataset(session)
        call_command(update_aws.Command(), '--hours', '24', verbosity=0)
        self.assertEqual(1, len(self.aws_calls))
        self.assertTrue(FileRecord.objects.filter(
            dataset=dataset, data_repository=self.aws['aws_mainenlab']).exists())


class TestFailureHandling(AWSSyncTests):
    """A directory that fails to sync must not be recorded as being on AWS."""

    def _session_dir(self, nickname='SWC_042'):
        return f'{self.root.as_posix()}/mainenlab/Subjects/{nickname}/2020-01-01/001'

    def test_failed_sync_does_not_create_aws_records(self):
        self.returncode = 1
        session = self._create_session()
        dataset = self._create_dataset(session, name='a.pqt')
        with self.assertRaises(CommandError):
            self._run('--hours', '24')
        self.assertFalse(FileRecord.objects
                         .filter(dataset=dataset, data_repository__name__startswith='aws')
                         .exists())

    def test_failed_sync_leaves_dataset_in_the_queue(self):
        self.returncode = 1
        session = self._create_session()
        dataset = self._create_dataset(session, name='a.pqt')
        with self.assertRaises(CommandError):
            self._run('--hours', '24')
        self.assertEqual(
            [dataset.pk], [r['dataset__id'] for r in self._records(['--hours', '24'])])

    def test_one_failure_does_not_block_other_sessions(self):
        """The other directories are still synced and recorded."""
        good = self._create_session(nickname='SWC_042')
        bad = self._create_session(nickname='SWC_043')
        good_dataset = self._create_dataset(good, name='a.pqt')
        bad_dataset = self._create_dataset(bad, name='b.pqt')
        self.fail_dirs = {self._session_dir('SWC_043')}
        with self.assertRaises(CommandError):
            self._run('--hours', '24')
        self.assertTrue(FileRecord.objects.filter(
            dataset=good_dataset, data_repository=self.aws['aws_mainenlab']).exists())
        self.assertFalse(FileRecord.objects.filter(
            dataset=bad_dataset, data_repository=self.aws['aws_mainenlab']).exists())

    def test_failure_does_not_record_a_completed_sync(self):
        """The end time stays unset, so the next --since-last run covers the window again."""
        self.returncode = 1
        session = self._create_session()
        self._create_dataset(session, name='a.pqt')
        with self.assertRaises(CommandError):
            self._run()
        syncs = pd.read_csv(self.sync_times_file, parse_dates=[0, 1])
        self.assertEqual(1, len(syncs))
        self.assertTrue(pd.isna(syncs.iloc[-1]['end']))

    def test_missing_destination_repository_is_a_failure(self):
        """A lab with no aws_<lab> repository is reported rather than silently skipped."""
        self.aws.pop('aws_mainenlab').delete()
        session = self._create_session()
        dataset = self._create_dataset(session, name='a.pqt')
        with self.assertRaises(CommandError):
            self._run('--hours', '24')
        self.assertEqual(1, len(self.aws_calls))
        self.assertFalse(FileRecord.objects
                         .filter(dataset=dataset, data_repository__name__startswith='aws')
                         .exists())


class TestConcurrency(AWSSyncTests):
    """--jobs runs several syncs at once; the outcome must not depend on it."""

    def _create_sessions(self, n):
        datasets = []
        for i in range(n):
            session = self._create_session(nickname=f'SWC_{i:03d}')
            datasets.append(self._create_dataset(session, name='a.pqt'))
        return datasets

    def _assert_all_synced(self, datasets):
        self.assertEqual(len(datasets), len(self.aws_calls))
        for dataset in datasets:
            record = FileRecord.objects.get(
                dataset=dataset, data_repository=self.aws['aws_mainenlab'])
            self.assertTrue(record.exists)

    def test_serial(self):
        datasets = self._create_sessions(4)
        self._run('--hours', '24', '--jobs', '1')
        self._assert_all_synced(datasets)

    def test_parallel(self):
        datasets = self._create_sessions(8)
        self._run('--hours', '24', '--jobs', '4')
        self._assert_all_synced(datasets)

    def test_parallel_with_a_failure(self):
        datasets = self._create_sessions(4)
        self.fail_dirs = {
            f'{self.root.as_posix()}/mainenlab/Subjects/SWC_002/2020-01-01/001'}
        with self.assertRaises(CommandError):
            self._run('--hours', '24', '--jobs', '4')
        synced = FileRecord.objects.filter(
            data_repository=self.aws['aws_mainenlab']).values_list('dataset_id', flat=True)
        self.assertCountEqual(
            [d.pk for i, d in enumerate(datasets) if i != 2], list(synced))


class TestMissingDirectories(AWSSyncTests):
    """Existence checks go through a directory listing cache, so absent folders must be safe."""

    def test_absent_session_directory(self):
        session = self._create_session()
        datasets = [self._create_dataset(session, name=f'a{i}.pqt', on_disk=False)
                    for i in range(3)]
        self._run('--hours', '24')
        for dataset in datasets:
            record = FileRecord.objects.get(
                dataset=dataset, data_repository=self.aws['aws_mainenlab'])
            self.assertFalse(record.exists)
            self.assertFalse(dataset.file_records.get(data_repository=self.flatiron).exists)

    def test_mixed_presence_in_one_directory(self):
        session = self._create_session()
        present = self._create_dataset(session, name='here.pqt', on_disk=True)
        absent = self._create_dataset(session, name='gone.pqt', on_disk=False)
        self._run('--hours', '24')
        self.assertTrue(FileRecord.objects.get(
            dataset=present, data_repository=self.aws['aws_mainenlab']).exists)
        self.assertFalse(FileRecord.objects.get(
            dataset=absent, data_repository=self.aws['aws_mainenlab']).exists)

    def test_directory_is_not_mistaken_for_the_file(self):
        """A folder named like the dataset file must not count as the file being present."""
        session = self._create_session()
        dataset = self._create_dataset(session, name='a.pqt', on_disk=False)
        record = dataset.file_records.get(data_repository=self.flatiron)
        path = Path(self.root.as_posix() + self.flatiron.globus_path + record.relative_path)
        add_uuid_string(path, dataset.pk).mkdir(parents=True)
        self._run('--hours', '24')
        self.assertFalse(FileRecord.objects.get(
            dataset=dataset, data_repository=self.aws['aws_mainenlab']).exists)


class TestQueryBudget(AWSSyncTests):
    """The query count must scale with directories synced, never with file records.

    The command runs over hundreds of thousands of file records on SDSC, so a query per record is
    the difference between minutes and hours. The row-by-row implementation issued ~17 each
    (``Dataset.objects.get``, ``get_or_create``, ``full_clean``'s unique check, ``save``, and the
    cascading ``Dataset.save`` behind it): 211 queries for the 12 records of the first case below
    and 415 for the 24 of the second. The bounds are deliberately tight. If a change pushes past
    them something has gone back to working per record, which is worth finding rather than
    raising the number for.
    """

    FIXED_QUERIES = 8
    QUERIES_PER_DIRECTORY = 3

    # Savepoints, which the command's per-directory transaction.atomic() only emits because
    # TestCase has already opened a transaction around the test. In production that atomic block
    # is the outermost one, so it is a plain BEGIN/COMMIT and costs no statements at all.
    SAVEPOINT_SQL = ('SAVEPOINT', 'RELEASE SAVEPOINT', 'ROLLBACK TO SAVEPOINT')

    def _run_and_count(self, n_sessions, n_datasets):
        for i in range(n_sessions):
            session = self._create_session(nickname=f'SWC_{i:03d}')
            for j in range(n_datasets):
                self._create_dataset(session, name=f'a{j}.pqt')
        with CaptureQueriesContext(connection) as ctx:
            self._run('--hours', '24')
        return sum(
            1 for query in ctx.captured_queries
            if not query['sql'].startswith(self.SAVEPOINT_SQL))

    def _assert_within_budget(self, n_sessions, n_datasets):
        n_queries = self._run_and_count(n_sessions, n_datasets)
        budget = self.FIXED_QUERIES + n_sessions * self.QUERIES_PER_DIRECTORY
        self.assertLessEqual(
            n_queries, budget,
            f'{n_queries} queries for {n_sessions} sessions of {n_datasets} datasets '
            f'({n_sessions * n_datasets} file records) exceeds the budget of {budget}')

    def test_query_count_for_several_sessions(self):
        self._assert_within_budget(3, 4)

    def test_query_count_does_not_grow_with_datasets_per_session(self):
        """Same session count as above, twice the file records, same budget."""
        self._assert_within_budget(3, 8)

    def test_query_count_for_one_large_session(self):
        """50 file records in one directory: the strongest form of the same assertion."""
        self._assert_within_budget(1, 50)

    def test_query_count_for_many_small_sessions(self):
        self._assert_within_budget(10, 1)

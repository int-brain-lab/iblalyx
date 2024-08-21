"""Generate an aggregate trials dataset for a given subject.

This command is currently set up to run on the Flatiron datauser account. To install, set up a link
to the alyx data app management commands folder:

>>> basedir="/home/datauser/Documents/github"
>>> ln -s "$basedir/iblalyx/management/commands/aggregate_subject_trials.py" \
... "$basedir/alyx/alyx/data/management/commands/aggregate_subject_trials.py"

Examples
--------

>>> python manage.py aggregate_subject_trials SWC_022 --dryrun

This produces the following files:
/mnt/ibl/aggregates/Subjects/mrsicflogellab/SWC_022/_ibl_subjectTrials.table.DRYRUN.pqt
/mnt/ibl/aggregates/Subjects/mrsicflogellab/SWC_022/_ibl_subjectTrials.log.DRYRUN.csv

>>> python manage.py aggregate_subject_trials SWC_022

This produces the following files and registers the parquet dataset to Alyx:
/mnt/ibl/aggregates/Subjects/mrsicflogellab/SWC_022/_ibl_subjectTrials.table.40b4f7aa-a72b-4f56-81f4-c2ef38976fbd.pqt
/mnt/ibl/aggregates/Subjects/mrsicflogellab/SWC_022/_ibl_subjectTrials.log.csv

"""

import logging
import hashlib
from pathlib import Path
from datetime import date
from collections import defaultdict

import pandas as pd
from one.alf.io import AlfBunch
import one.alf.files as alfiles
from one.alf.spec import is_uuid_string
from iblutil.io import hashfile

import ibllib.pipes.dynamic_pipeline as dyn

from django.core.management import BaseCommand
from subjects.models import Subject
from actions.models import Session
from misc.models import LabMember
from data.models import Dataset, DataRepository, FileRecord, DataFormat, DatasetType, Revision

logger = logging.getLogger('ibllib')
ROOT = Path('/mnt/ibl')
OUTPUT_PATH = ROOT / 'aggregates'
VERSION = 1.1  # The dataset version (NB: change after dataset extraction modifications)
EXPECTED_KEYS = {
    # Trials table keys
    'intervals_0', 'intervals_1', 'goCue_times', 'response_times', 'choice', 'stimOn_times',
    'contrastLeft', 'contrastRight', 'feedback_times', 'feedbackType', 'rewardVolume', 'probabilityLeft', 'firstMovement_times',
    # Additional trials data
    'goCueTrigger_times', 'stimOnTrigger_times', 'stimFreezeTrigger_times', 'stimFreeze_times', 'stimOffTrigger_times',
    'stimOff_times', 'phase', 'position', 'quiescence',
    # Session meta data
    'session', 'session_start_time', 'task_protocol', 'protocol_number'
}


def get_protocol_number(task):
    """
    Get the task protocol number from a behaviour task instance.

    Parameters
    ----------
    task : ibllib.pipes.base_tasks.BehaviourTask
        A behaviour task instance.

    Returns
    -------
    int
        The task protocol number.
    """
    # Get protocol number (usually 0)
    collection = task.collection
    try:
        n = task.protocol_number or (0 if collection == 'raw_behavior_data' else int(collection.split('_')[-1]))
    except Exception:
        logger.warning('Failed to parse task collection %s', task.collection)
        n = 0
    return n


def load_pipeline_tasks(subject, sessions, root, outcomes=None):
    """
    Instantiate all trials-related pipeline tasks for a given set of sessions.

    Parameters
    ----------
    subject : str
        The subject nickname.
    sessions : pandas.DataFrame
        A sorted dataframe with the columns {'id', 'start_time', 'number', 'lab'}.
    root : pathlib.Path
        The root folder containing the data.
    outcomes : list of tuple
        An optional list to append to.

    Returns
    -------
    dict
        Map of session UUID to list of trials tasks.
    set of pathlib.Path
        A set of input files used by the extractor tasks.
    list of tuple
        A list of extraction outcomes (session uuid, task number, notes).
    """
    outcomes = outcomes or []
    all_tasks = defaultdict(list)
    input_files = set()
    for i, info in sessions.iterrows():
        session_path = root.joinpath(info.lab, 'Subjects', subject, str(info.start_time.date()), str(info.number).zfill(3))
        logger.info('Session %i/%i: %s', i + 1, len(sessions), session_path)
        if not session_path.exists():
            logger.error('Session path does not exist')
            outcomes.append((info.id, -1, 'session path does not exist'))
            continue
        try:
            tasks = dyn.get_trials_tasks(session_path)
            assert len(tasks) > 0, 'no tasks returned for session'
        except Exception as ex:
            logger.error(ex)
            outcomes.append((info.id, -1, 'failed to get trials tasks'))
            continue
        tasks = list(filter(dyn.is_active_trials_task, tasks))
        if len(tasks) == 0:
            # TODO I assume there are valid instances of this, e.g. passive sessions still have raw trial data?
            logger.info('No trials tasks for this session')
            outcomes.append((info.id, -1, 'no trials tasks for this session'))
            continue
        for task in tasks:
            # Ensure input files exist
            task.location = task.machine = 'sdsc'
            task.get_signatures()
            inputs_present, inputs = task.assert_expected_inputs(raise_error=False)
            proc_number = get_protocol_number(task)
            if not inputs_present:
                logger.error('%s: one or more input files missing', task.name)
                outcomes.append((info.id, proc_number, 'one or more input files missing'))
                continue
            input_files.update(inputs)
            all_tasks[info.id].append(task)
            outcomes.append((info.id, proc_number, 'INITIALIZED'))

    return all_tasks, input_files, outcomes


def generate_trials_aggregate(session_tasks: dict, outcomes=None):
    """
    Extract trials tables for a given set of sessions.

    Parameters
    ----------
    session_tasks : dict
        Ordered map of session UUID to list of BehaviourTask instances.
    outcomes : list of tuple
        An optional list to append to.

    Returns
    -------
    pandas.DataFrame
        A sorted dataframe with trials columns.
    list of tuple
        A list of extraction outcomes (session uuid, task number, notes).
    """
    all_trials = []
    outcomes = outcomes or []
    expected = {
        'goCueTrigger_times', 'stimOnTrigger_times', 'stimFreezeTrigger_times', 'stimFreeze_times',
        'stimOffTrigger_times', 'stimOff_times', 'phase', 'position', 'quiescence', 'table'}
    for i, (eid, tasks) in enumerate(session_tasks.items()):
        logger.info('Session %i/%i: %s', i + 1, len(session_tasks), tasks[0].session_path)
        for task in tasks:
            proc_number = get_protocol_number(task)
            try:
                trials, _ = task.extract_behaviour(save=False)
            except Exception as ex:
                msg = f'failed to extract trials: {ex}'
                logger.error(msg.capitalize())
                outcomes.append((eid, proc_number, msg))
                continue
            if not expected.issubset(trials):
                msg = 'missing extracted vars: ' + '", "'.join(expected - set(trials))
                logger.error(msg)
                outcomes.append((eid, proc_number, msg))
                continue
            # Convert to trials frame and add to stack
            for key in set(trials.keys()) - expected:  # Remove unnecessary keys
                del trials[key]
            trials = trials.pop('table').join(AlfBunch(trials).to_df())  # Convert to frame
            trials['session'] = str(eid)
            trials['task_protocol'] = task.protocol
            trials['protocol_number'] = proc_number
            # trials['session_start_time'] = info.start_time
            all_trials.append(trials)
            outcomes.append((eid, proc_number, 'SUCCESS'))
    df_trials = pd.concat(all_trials, ignore_index=True)
    return df_trials, outcomes


class Command(BaseCommand):
    """Generate an aggregate trials dataset for a given subject."""
    help = "Generate an aggregate trials dataset for a given subject."
    subject = None
    """subjects.models.Subject: The subject to aggregate."""
    output_path = None
    """pathlib.Path: Where to save the aggregate dataset."""

    def add_arguments(self, parser):
        parser.add_argument('subject', type=str, help='A subject nickname.')
        parser.add_argument('--output-path', type=Path, default=OUTPUT_PATH,
                            help='The save location of the aggregate dataset.')
        parser.add_argument('--dryrun', action='store_true',
                            help='Runs aggregate table generation without registration. Output file contains "DRYRUN".')
        parser.add_argument('--data-path', type=Path, default=ROOT,
                            help='The root data path containing subject data.')
        parser.add_argument('--alyx-user', type=str, default='root',
                            help='The alyx user under which to register the dataset.')
        parser.add_argument('--clobber', action='store_true', default=False,
                            help='If passed, force a regeneration of the dataset.')

    def handle(self, *_, **options):
        # Unpack options
        verbosity = options.pop('verbosity')
        if verbosity < 1:
            logger.setLevel(logging.WARNING)
        elif verbosity == 1:
            logger.setLevel(logging.INFO)
        elif verbosity > 1:
            logger.setLevel(logging.DEBUG)

        dry, subject, user = options['dryrun'], options['subject'], options['alyx_user']
        self.subject = Subject.objects.get(nickname=subject)
        self.output_path = options['output_path']
        query = self.query_sessions(self.subject)
        # Create sessions dataframe
        fields = ('id', 'start_time', 'number')
        sessions = pd.DataFrame.from_records(query.values(*fields).distinct())
        sessions['lab'] = lab_name = self.subject.lab.name
        out_file = self.output_path.joinpath('Subjects', lab_name, subject, '_ibl_subjectTrials.table.pqt')

        # Check whether new aggregate required
        # First checking number of sessions, then number of input files, then input file sizes, then the hashes
        qs = Dataset.objects.filter(
            name='_ibl_subjectTrials.table.pqt', object_id=self.subject.id, default_dataset=True)
        all_tasks, input_files, outcomes = load_pipeline_tasks(subject, sessions, options['data_path'])
        rerun = options['clobber'] is True or qs.count() == 0
        if rerun:
            logger.info('Forcing re-run' if options['clobber'] else 'No previous aggregate dataset')
        else:
            # Attempt to load
            old_aggregate = alfiles.add_uuid_string(out_file, qs.first().id)
            rerun |= not old_aggregate.exists()
            if rerun:
                logger.info('Previous aggregate file missing on disk: %s', old_aggregate)
            else:
                old_trials = pd.read_parquet(old_aggregate)
                rerun |= set(sessions['id'].astype(str).unique()) != set(old_trials['session'].unique())
                # If there are more sessions to extract now, do the re-run
                to_extract = {str(eid) for eid, _, note in outcomes if note == 'INITIALIZED'}
                rerun |= to_extract > set(old_trials['session'].unique())
                if rerun:
                    logger.info('Set of sessions changed')
                else:
                    aggregate_hash = self.make_aggregate_hash(to_extract, input_files=input_files)
                    rerun |= aggregate_hash != (qs.first().json or {}).get('aggregate_hash')
                    if rerun:
                        logger.info('Aggregate hash changed')

        if not rerun:
            logger.info('Aggregate hash unchanged; exiting')
            return

        # Generate aggregate trials table
        all_trials, outcomes = generate_trials_aggregate(all_tasks, outcomes)
        outcomes = pd.DataFrame(outcomes, columns=['session', 'task number', 'notes'])
        outcomes.drop_duplicates(['session', 'task number'], keep='last', inplace=True)
        # Add start_times column to trials table
        start_times = sessions.astype({'id': str}).set_index('id')['start_time'].rename('session_start_time')
        all_trials = all_trials.merge(start_times, left_on='session', right_index=True, sort=False)
        assert set(all_trials.columns) == set(EXPECTED_KEYS), 'unexpected columns in aggregate trials table'

        if out_file.exists():
            logger.warning(('(DRY) ' if dry else '') + 'Output file already exists, overwriting %s', out_file)
        if dry:
            out_file = out_file.with_name(f'{out_file.stem}.DRYRUN{out_file.suffix}')
        # Save to disk
        out_file.parent.mkdir(parents=True, exist_ok=True)
        all_trials.to_parquet(out_file)
        assert out_file.exists(), f'Failed to save to {out_file}'
        assert (file_size := out_file.stat().st_size) > 0
        assert not pd.read_parquet(out_file).empty, f'Failed to read-after-write {out_file}'
        md5_hash = hashfile.md5(out_file)
        # Save outcome log
        log_file = out_file.with_name(f'_ibl_subjectTrials.log{".DRYRUN" if dry else ""}.csv')
        outcomes.to_csv(log_file)

        # Create aggregate hash
        successful_sessions = outcomes['session'][outcomes['notes'] == 'SUCCESS'].unique().tolist()
        aggregate_hash = self.make_aggregate_hash(successful_sessions, input_files=input_files)

        if dry:
            logger.info('Dry run complete: %s aggregate hash = %s', out_file, aggregate_hash)
            return None, out_file, log_file

        # Create dataset
        dset, out_file = self.register_dataset(
            out_file, file_hash=md5_hash, file_size=file_size, aggregate_hash=aggregate_hash, user=user)
        return dset, out_file, log_file

    @staticmethod
    def files2hash(file_list):
        """Return list of file hashes from file path list.

        This function first attempts to get the hash from the Alyx dataset record, and if not found
        calculates the hash from disk.

        Parameters
        ----------
        file_list : list of pathlib.Path
            A list of dataset file paths.

        Returns
        -------
        list of str
            A list of dataset file hashes.
        """
        dids = [next((x for x in f.name.split('.') if is_uuid_string(x)), None) for f in file_list]
        alyx_hashes = Dataset.objects.filter(pk__in=set(filter(None, dids))).values('pk', 'hash')
        did2hash = {str(x['pk']): x['hash'] for x in alyx_hashes}
        return [did2hash.get(did) or hashfile.md5(file) for did, file in zip(dids, file_list)]

    @staticmethod
    def make_aggregate_hash(sessions, input_files=None):
        """
        Compute hash of session datasets.

        Creates hash of concatenated dataset UUIDs and their file hashes. The datasets used are the
        raw settings, raw trials jsonable, experiment description, and extracted trials table. The
        assumption is that if any of these changes, so should the aggregate table, however the
        table may require regenerating even when the hash remains the same.

        Parameters
        ----------
        sessions : list of uuid.UUID, list of str
            List of session UUIDs that are present in aggregate table.
        input_files : list of pathlib.Path
            A list of trials task input files to use in creating the aggregate hash.

        Returns
        -------
        str
            An aggregate MD5 hash.
        """
        # At minimum include trials table, which will differ if extraction code has changed
        trials_ds = Dataset.objects.filter(
            session__in=sessions, default_dataset=True, name='_ibl_trials.table.pqt')

        if input_files:
            logger.info('Hashing input files')
            input_files = sorted(set(input_files))  # Ensure always same order
            assert all(map(Path.exists, input_files)), 'not all input files exist'
            trials_hashes = filter(None, trials_ds.order_by('hash').values_list('hash', flat=True))
            inputs_hashes = Command.files2hash(input_files)
            hash_str = ''.join((*inputs_hashes, *trials_hashes)).encode('utf-8')
            new_hash = hashlib.md5(hash_str).hexdigest()
        else:  # Old way of calculating the aggregate hash
            # For sessions that have a trials table, add the task data files
            raw_task_ds = Dataset.objects.filter(
                session__in=sessions, default_dataset=True,
                name__in=['_iblrig_taskSettings.raw.json', '_iblrig_taskData.raw.jsonable', '_ibl_experiment.description.yaml'])
            # If we don't have task data for each session, that's a problem. This check is a little unnecessary
            assert len(set(raw_task_ds.values_list('session', flat=True))) == len(sessions), 'not all sessions have raw task data'
            # At minimum include trials table, which will differ if extraction code has changed
            trials_ds = Dataset.objects.filter(
                session__in=sessions, default_dataset=True, name='_ibl_trials.table.pqt')
            # Compute the hash
            hash_ds = (trials_ds | raw_task_ds).order_by('hash')
            hash_str = ''.join(str(item) for pair in hash_ds.values_list('hash', 'id') for item in pair).encode('utf-8')

            new_hash = hashlib.md5(hash_str).hexdigest()

        return new_hash

    def register_dataset(self, file_path, file_hash=None, aggregate_hash=None, file_size=None, user='root', revision=None):
        """
        Register an aggregate trials table dataset.

        Parameters
        ----------
        file_path : pathlib.Path
            An aggregate trials table file to register.
        file_hash : str
            The MD5 hash of the file.
        aggregate_hash : str
            An MD5 hash of dependent dataset hashes.
        file_size : int
            The file size in bytes.
        user : str, User
            The Alyx database user registering the dataset.
        revision : str, Revision
            The revision name to use for the dataset.

        Returns
        -------
        Dataset
            The Dataset record for the aggregate table.
        pathlib.Path
            The output file path.
        """
        if not file_path.exists():
            raise FileNotFoundError(file_path)
        assert all((self.output_path, self.subject)), 'subject and output path must be set'
        # Get or create the dataset
        dset, is_new = Dataset.objects.get_or_create(
            name='_ibl_subjectTrials.table.pqt', collection='Subjects', default_dataset=True,
            dataset_type=DatasetType.objects.get(name='subjectTrials.table'),
            data_format=DataFormat.objects.get(name='parquet'), object_id=self.subject.id)

        # Check if unchanged; whether new revision is required
        if revision or not is_new:
            unchanged = (
                    file_hash and file_hash == dset.hash
                    and aggregate_hash == (dset.json or {}).get('aggregate_hash')
                    and file_size == dset.file_size)
            if revision or (dset.is_protected and not unchanged):
                revision = revision or date.today().isoformat()
                assert dset.revision is None or dset.revision.name != revision, \
                    f'Unable to overwrite protected dataset with revision "{revision}".'
                if isinstance(revision, str):  # user may have already passed in revision obj
                    revision, _ = Revision.objects.get_or_create(name=revision)
                logger.info('Creating new dataset with "%s" revision', revision.name)
                # Create new dataset; leave the old untouched (save method handles change of default dataset field)
                dset = Dataset.objects.create(
                    name='_ibl_subjectTrials.table.pqt', collection='Subjects', default_dataset=True,
                    dataset_type=DatasetType.objects.get(name='subjectTrials.table'),
                    data_format=DataFormat.objects.get(name='parquet'), content_object=self.subject, revision=revision
                )
                is_new = True

        # Update fields with file info and move file to new location
        dset.version = VERSION
        dset.file_size = file_size
        dset.hash = file_hash
        dset.user = LabMember.objects.get(username=user) if isinstance(user, str) else user
        dset.json = {**(dset.json or {}), 'aggregate_hash': aggregate_hash}
        logger.info(('Created' if is_new else 'Updated') + ' aggregate dataset with UUID %s', dset.pk)
        # Validate dataset
        dset.full_clean()
        dset.save()

        out_file = self.output_path.joinpath(dset.collection, self.subject.lab.name, self.subject.nickname)
        if dset.revision:
            out_file /= dset.revision.name
        out_file /= alfiles.add_uuid_string('_ibl_subjectTrials.table.pqt', dset.pk)
        if out_file.exists():
            logger.warning('Output file %s already exists, overwriting', out_file)
        logger.info('%s -> %s', file_path, out_file)
        out_file = file_path.replace(out_file)
        assert out_file.exists(), f'failed to move {file_path} to {out_file}'

        # Update file records
        for repo in map(DataRepository.objects.get, ('aws_aggregates', 'flatiron_aggregates')):
            kwargs = {'exists': repo.name.startswith('flatiron')}
            r, r_is_new = FileRecord.objects.update_or_create(
                dataset=dset, data_repository=repo, relative_path=out_file.relative_to(self.output_path).as_posix(),
                defaults=kwargs, create_defaults=kwargs)
            logger.debug(('Created' if r_is_new else 'Updated') + ' file record %s: exists=%s', r.name, r.exists)

        return dset, out_file

    @staticmethod
    def query_sessions(subject):
        """Query all subject sessions with raw task data."""
        sessions = (
            Session
            .objects
            .prefetch_related('data_dataset_session_related')
            .filter(subject=subject, data_dataset_session_related__name='_iblrig_taskData.raw.jsonable', type='Experiment')
            .order_by('start_time', 'number'))
        return sessions

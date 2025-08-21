"""Generate an aggregate trials dataset for a given subject.

This command is currently set up to run on the Flatiron datauser account. To install, set up a link
to the alyx data app management commands folder:

>>> basedir="/home/datauser/Documents/PYTHON"
>>> ln -s "$basedir/iblalyx/management/commands/aggregate_subject_trials.py" \
... "$basedir/alyx/alyx/data/management/commands/aggregate_subject_trials.py"
>>> ln -s "$basedir/iblalyx/management/one_django.py" \
... "$basedir/alyx/alyx/data/management/one_django.py"


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
from itertools import chain

import numpy as np
import pandas as pd
from one.alf.io import AlfBunch
import one.alf.path as alfiles
from one.alf.spec import is_uuid_string
from one.alf.cache import SESSIONS_COLUMNS
from iblutil.io import hashfile

import ibllib.pipes.dynamic_pipeline as dyn
from ibllib import __version__ as ibllib_version
import ibllib.pipes.training_status as ts

from django.core.management import BaseCommand
from django.contrib.postgres.aggregates import ArrayAgg
from subjects.models import Subject
from actions.models import Session
from misc.models import LabMember
from data.models import Dataset, DataRepository, FileRecord, DataFormat, DatasetType, Revision
from data.management.one_django import OneDjango, CACHE_DIR_FI as ROOT

logger = logging.getLogger('ibllib')
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


def load_pipeline_tasks(subject, sessions, root, outcomes=None, one=None):
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
    one : one.api.One
        An instance of ONE to use by the data handler for loading the input datasets. On SDSC this
        creates symlinks of the default revisions.

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
            task.one = one
            task.data_handler = task.get_data_handler()
            task.data_handler.setUp(task)
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
        logger.info('=== Session %i/%i: %s ===', i + 1, len(session_tasks), tasks[0].session_path)
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
            if getattr(task, 'extractor') is not None:
                del(task.extractor)
    df_trials = pd.concat(all_trials, ignore_index=True)
    return df_trials, outcomes


def generate_training_aggregate(training_file, subject, root=ROOT):
    """
    Compute training criteria table for a give subject

    Parameters
    ----------
    training_file : pathlib.Path
        The path to the subjectTrials.table file
    subject : subjects.models.Subject
        The subject to compute the training criteria for
    root : pathlib.Path
        The root folder containing the data.


    Returns
    -------
    pandas.DataFrame
        A sorted dataframe with training criteria
    """

    # Load in the subjectTrials.table
    subj_df = pd.read_parquet(training_file)

    # Find the dates that we need to compute the training status for
    missing_dates = pd.DataFrame()
    path2eid = dict()
    for eid, info in subj_df.groupby('session'):
        sess = Session.objects.get(id=eid)
        session_path = root.joinpath(subject.lab.name, 'Subjects', subject.nickname, str(sess.start_time.date()),
                                     str(sess.number).zfill(3))
        path2eid[str(session_path)] = eid
        s_df = pd.DataFrame({'date': session_path.parts[-2], 'session_path': str(session_path)}, index=[0])
        missing_dates = pd.concat([missing_dates, s_df], ignore_index=True)

    missing_dates = missing_dates.sort_values('date')

    df = None
    # Iterate through the dates to fill up our training dataframe
    for _, grp in missing_dates.groupby('date'):
        sess_dicts = ts.get_training_info_for_session(grp.session_path.values, None, force=False)
        if len(sess_dicts) == 0:
            continue

        for sess_dict in sess_dicts:
            if df is None:
                df = pd.DataFrame.from_dict(sess_dict)
            else:
                df = pd.concat([df, pd.DataFrame.from_dict(sess_dict)])

    # Sort values by date and reset the index
    df = df.sort_values('date')
    df = df.reset_index(drop=True)

    # Add eids to the subject training table
    eids = []
    for sess in df.session_path.values:
        eids.append(path2eid[str(sess)])
    df['session'] = eids

    # Now go through the backlog and compute the training status for sessions.
    # If for example one was missing as it is cumulative
    # we need to go through and compute all the backlog
    # Find the earliest date in missing dates that we need to recompute the training status for
    missing_status = ts.find_earliest_recompute_date(df.drop_duplicates('date').reset_index(drop=True))
    for missing_date in missing_status:
        df, _, _, _ = ts.compute_training_status(df, missing_date, None, force=False)

    # Add in untrainable or unbiasable
    df_lim = df.drop_duplicates(subset='session', keep='first')
    # Detect untrainable
    un_df = df_lim[df_lim['training_status'] == 'in training'].sort_values('date')
    if len(un_df) >= 40:
        sess = un_df.iloc[39].session
        df.loc[df['session'] == sess, 'training_status'] = 'untrainable'

    # Detect unbiasable
    un_df = df_lim[df_lim['task_protocol'] == 'biased'].sort_values('date')
    if len(un_df) >= 40:
        tr_st = un_df[0:40].training_status.unique()
        if 'ready4ephysrig' not in tr_st:
            sess = un_df.iloc[39].session
            df.loc[df['session'] == sess, 'training_status'] = 'unbiasable'

    # Remove duplicate rows
    status = df.set_index('date')[['training_status', 'session']].drop_duplicates(subset='training_status',
                                                                                  keep='first')
    return status


def generate_session_aggregate(training_file):
    """
    Compute sessions table for a given subject

    Parameters
    ----------
    training_file : pathlib.Path
        The path to the subjectTrials.table file

    Returns
    -------
    pandas.DataFrame
        A dataframe with session metadata
    """

    trials_table = pd.read_parquet(training_file)
    sessions = trials_table.session.unique()

    fields = ('id', 'lab__name', 'subject__nickname', 'start_time__date',
              'number', 'task_protocol', 'all_projects')

    query = (Session
             .objects
             .filter(id__in=sessions)
             .select_related('subject', 'lab')
             .prefetch_related('projects')
             .annotate(all_projects=ArrayAgg('projects__name'))
             .order_by('-start_time', 'subject__nickname', '-number'))

    if query.count() == 0:
        return pd.DataFrame(columns=SESSIONS_COLUMNS).set_index('id')

    df = pd.DataFrame.from_records(query.values(*fields).distinct())

    # Rename, sort fields
    df['all_projects'] = df['all_projects'].map(lambda x: ','.join(filter(None, set(x))))
    # task_protocol & projects columns may be empty; ensure None -> ''
    # id UUID objects -> str; not supported by parquet
    df = (
        (df
         .rename(lambda x: x.split('__')[0], axis=1)
         .rename({'start_time': 'date', 'all_projects': 'projects'}, axis=1)
         .dropna(subset=['number', 'date', 'subject', 'lab'])  # Remove dud or base sessions
         .sort_values(['date', 'subject', 'number'], ascending=False)
         .astype({'number': np.uint16, 'task_protocol': str, 'projects': str, 'id': str}))
    )
    df.set_index('id', inplace=True)

    return df


class Command(BaseCommand):
    """Generate an aggregate trials and optionally training dataset(s) for a given subject."""

    help = "Generate an aggregate trials dataset for a given subject."
    subject = None
    """subjects.models.Subject: The subject to aggregate."""
    output_path = None
    """pathlib.Path: Where to save the aggregate dataset."""
    default_revision = None
    """str | Revision: The default revision to use if the current default dataset is protected."""
    revision = None
    """str | Revision: A specific revision to use. Even if the current default dataset is not protected a new dataset
     will be created with this revision."""
    user = None
    """str | User: The user to register the new datasets."""

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
        parser.add_argument('--revision', type=str,
                            help='The revision name to register dataset under.')
        parser.add_argument('--default-revision', type=str,
                            help='The default revision name to use if the current default dataset is protected.')
        parser.add_argument('--training-status', action='store_true', default=False,
                            help='If passed, the training status aggregate dataset is computed and registered.')

    def handle(self, *args, **options):
        # Unpack options
        verbosity = options.pop('verbosity', 1)
        if verbosity < 1:
            logger.setLevel(logging.WARNING)
        elif verbosity == 1:
            logger.setLevel(logging.INFO)
        elif verbosity > 1:
            logger.setLevel(logging.DEBUG)

        self.default_revision = options.pop('default_revision', None)
        options['subject'] = args[0] if len(args) else options['subject']
        dsets, files, log = self.run(**options)

        return dsets, files, log

    def run(self, subject, revision=None, output_path=OUTPUT_PATH, data_path=ROOT,
            dryrun=True, clobber=False, alyx_user='root', training_status=False, **kwargs):
        self.subject = Subject.objects.get(nickname=subject)
        self.user = alyx_user
        self.revision = revision
        self.output_path = output_path
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
        all_tasks, input_files, outcomes = load_pipeline_tasks(subject, sessions, data_path, one=OneDjango())
        rerun = clobber is True or qs.count() == 0
        if rerun:
            logger.info('Forcing re-run' if clobber else 'No previous aggregate dataset')
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
            for task in chain.from_iterable(all_tasks.values()):
                task.cleanUp()
            logger.info('Aggregate hash unchanged; exiting')
            if training_status:
                # The trials table may not need to be rerun, but we need to check if the training status table exists
                training_dset, training_out_file = self.handle_training_status(trials_table=None, rerun=rerun, dry=dryrun)
                return (None, training_dset), (None, training_out_file), None
            else:
                return (None, None), (None, None), None

        # Generate aggregate trials table
        all_trials, outcomes = generate_trials_aggregate(all_tasks, outcomes)
        outcomes = pd.DataFrame(outcomes, columns=['session', 'task number', 'notes'])
        outcomes.drop_duplicates(['session', 'task number'], keep='last', inplace=True)

        # Add start_times column to trials table
        start_times = sessions.astype({'id': str}).set_index('id')['start_time'].rename('session_start_time')
        all_trials = all_trials.merge(start_times, left_on='session', right_index=True, sort=False)
        assert set(all_trials.columns) == set(EXPECTED_KEYS), 'unexpected columns in aggregate trials table'

        if out_file.exists():
            logger.warning(('(DRY) ' if dryrun else '') + 'Output file already exists, overwriting %s', out_file)
        if dryrun:
            out_file = out_file.with_name(f'{out_file.stem}.DRYRUN{out_file.suffix}')

        # Save to disk
        out_file.parent.mkdir(parents=True, exist_ok=True)
        all_trials.to_parquet(out_file)
        assert out_file.exists(), f'Failed to save to {out_file}'
        assert (file_size := out_file.stat().st_size) > 0
        assert not pd.read_parquet(out_file).empty, f'Failed to read-after-write {out_file}'
        md5_hash = hashfile.md5(out_file)

        # Save outcome log
        log_file = out_file.with_name(f'_ibl_subjectTrials.log{".DRYRUN" if dryrun else ""}.csv')
        outcomes.to_csv(log_file)

        # Create aggregate hash
        successful_sessions = outcomes['session'][outcomes['notes'] == 'SUCCESS'].unique().tolist()
        aggregate_hash = self.make_aggregate_hash(successful_sessions, input_files=input_files)

        # Clean up all symlinks made by the data handler
        for task in chain.from_iterable(all_tasks.values()):
            task.cleanUp()

        # Create the session table
        session_dset, session_out_file = self.handle_session_table(trials_table=out_file, rerun=rerun, dry=dryrun)

        if training_status:
            training_dset, training_out_file = self.handle_training_status(trials_table=out_file, rerun=rerun, dry=dryrun)
        else:
            training_dset = training_out_file = None

        if dryrun:
            logger.info('Dry run complete: %s aggregate hash = %s', out_file, aggregate_hash)
            return (None, None, None), (out_file, training_out_file, session_out_file), log_file

        # Create dataset
        dset, out_file = self.register_dataset(
            out_file, file_hash=md5_hash, file_size=file_size, aggregate_hash=aggregate_hash, user=self.user,
            revision=self.revision)

        # Move log file to new revision folder
        if out_file.parent != log_file.parent:
            log_file = log_file.rename(out_file.with_name(log_file.name))

        logger.info('Command run complete')
        return (dset, training_dset, session_dset), (out_file, training_out_file, session_out_file), log_file

    def handle_training_status(self, trials_table=None, rerun=False, dry=False):
        """
        Compute subject training criteria dataset

        Parameters
        ----------
        trials_table : pathlib.Path or None
            Path to the subjectTrials.table dataset on disk, the sessions to use in the training status computation
            are taken from this file
        rerun : bool
            Indicates if the subjectTrials table has been newly created
        dry: bool
            Runs aggregate table generation without registration. Output file contains "DRYRUN"

        Returns
        -------
        Dataset
            The Dataset record for the aggregate table.
        pathlib.Path
            The output file path.
        """

        # Try except as there are cases where it fails, and we don't want that to stop the trials table registration
        try:
            qs = Dataset.objects.filter(
                name='_ibl_subjectTraining.table.pqt', object_id=self.subject.id, default_dataset=True)

            # If a dataset already exists and rerun is false, we do not do anything
            if qs.count() > 0 and not rerun:
                logger.info(f'Training status file exists and rerun is {rerun}; exiting')
                return None, None

            # Otherwise either the dataset doesn't exist or needs to be recomputed
            # If the trials table file hasn't just been created we need to find the relevant file
            if trials_table is None:
                dset = Dataset.objects.get(name='_ibl_subjectTrials.table.pqt', object_id=self.subject.id,
                                           default_dataset=True)
                trials_table = self.output_path.joinpath(
                    alfiles.add_uuid_string(dset.file_records.all()[0].relative_path, dset.pk))

            assert trials_table.exists()

            # Compute the training status table
            training_status = generate_training_aggregate(trials_table, self.subject)

            # Specify the file to save to
            out_file = self.output_path.joinpath('Subjects', self.subject.lab.name, self.subject.nickname,
                                                 '_ibl_subjectTraining.table.pqt')

            if out_file.exists():
                logger.warning(('(DRY) ' if dry else '') + 'Output file already exists, overwriting %s', out_file)
            if dry:
                out_file = out_file.with_name(f'{out_file.stem}.DRYRUN{out_file.suffix}')

            # Save the training status to file
            out_file.parent.mkdir(parents=True, exist_ok=True)
            training_status.to_parquet(out_file)
            assert out_file.exists(), f'Failed to save to {out_file}'
            assert (file_size := out_file.stat().st_size) > 0
            assert not pd.read_parquet(out_file).empty, f'Failed to read-after-write {out_file}'
            md5_hash = hashfile.md5(out_file)

            if dry:
                logger.info('Dry run complete: %s', out_file)
                return None, out_file

            # Format the training criteria into a dict to update the Subject json
            criteria_dates, sess = training_status.to_dict().items()
            trained_criteria = {v.replace(' ', '_'): (k, sess[1][k]) for k, v in criteria_dates[1].items()}

            # Update the json field of the subject with the new training dates
            self.subject.json = {**(self.subject.json or {}), 'trained_criteria': trained_criteria}
            self.subject.save()

            # Register the dataset
            dset, out_file = self.register_dataset(
                out_file, file_hash=md5_hash, file_size=file_size, user=self.user, revision=self.revision)

        except Exception as err:
            logger.error(f'Training status aggregate generation failed with error: {err}')
            dset = out_file = None

        return dset, out_file

    def handle_session_table(self, trials_table=None, rerun=False, dry=False):
        try:
            qs = Dataset.objects.filter(
                name='_ibl_subjectSessions.table.pqt', object_id=self.subject.id, default_dataset=True)

            # If a dataset already exists and rerun is false, we do not do anything
            if qs.count() > 0 and not rerun:
                logger.info(f'Session table file exists and rerun is {rerun}; exiting')
                return None, None

            # Otherwise either the dataset doesn't exist or needs to be recomputed
            # If the trials table file hasn't just been created we need to find the relevant file
            if trials_table is None:
                dset = Dataset.objects.get(name='_ibl_subjectTrials.table.pqt', object_id=self.subject.id,
                                           default_dataset=True)
                trials_table = self.output_path.joinpath(
                    alfiles.add_uuid_string(dset.file_records.all()[0].relative_path, dset.pk))

            assert trials_table.exists()

            # Generate the subject sessions table
            sessions_table = generate_session_aggregate(trials_table)

            # Specify the file to save to
            out_file = self.output_path.joinpath('Subjects', self.subject.lab.name, self.subject.nickname,
                                                 '_ibl_subjectSessions.table.pqt')

            if out_file.exists():
                logger.warning(('(DRY) ' if dry else '') + 'Output file already exists, overwriting %s', out_file)
            if dry:
                out_file = out_file.with_name(f'{out_file.stem}.DRYRUN{out_file.suffix}')

            # Save the training status to file
            out_file.parent.mkdir(parents=True, exist_ok=True)
            sessions_table.to_parquet(out_file)
            assert out_file.exists(), f'Failed to save to {out_file}'
            assert (file_size := out_file.stat().st_size) > 0
            assert not pd.read_parquet(out_file).empty, f'Failed to read-after-write {out_file}'
            md5_hash = hashfile.md5(out_file)

            if dry:
                logger.info('Dry run complete: %s', out_file)
                return None, out_file

            # Register the dataset
            dset, out_file = self.register_dataset(
                out_file, file_hash=md5_hash, file_size=file_size, user=self.user, revision=self.revision)

        except Exception as err:
            logger.error(f'Session table aggregate generation failed with error: {err}')
            dset = out_file = None

        return dset, out_file

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
        Register an aggregate subject table dataset.

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
            The revision name to use for the dataset (NB: do not add pound signs to name).

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
        collection = f'Subjects/{self.subject.lab.name}/{self.subject.nickname}'
        # Get the alf object from the filename
        alf_object = alfiles.filename_parts(file_path.name, as_dict=True)['object']

        # Get or create the dataset
        dset, is_new = Dataset.objects.get_or_create(
            name=f'_ibl_{alf_object}.table.pqt', collection=collection, default_dataset=True,
            dataset_type=DatasetType.objects.get(name=f'{alf_object}.table'),
            data_format=DataFormat.objects.get(name='parquet'), object_id=self.subject.id)

        # Check if unchanged; whether new revision is required
        if revision or not is_new:
            unchanged = (
                    file_hash and file_hash == dset.hash
                    and aggregate_hash == (dset.json or {}).get('aggregate_hash')
                    and file_size == dset.file_size)
            if revision or (dset.is_protected and not unchanged):
                revision = revision or self.default_revision or date.today().isoformat()
                assert dset.revision is None or dset.revision.name != revision, \
                    f'Unable to overwrite protected dataset with revision "{revision}".'
                if isinstance(revision, str):  # user may have already passed in revision obj
                    revision, _ = Revision.objects.get_or_create(name=revision)
                logger.info('Creating new dataset with "%s" revision', revision.name)
                # Create new dataset; leave the old untouched (save method handles change of default dataset field)
                dset = Dataset.objects.create(
                    name=f'_ibl_{alf_object}.table.pqt', collection=collection, default_dataset=True,
                    dataset_type=DatasetType.objects.get(name=f'{alf_object}.table'),
                    data_format=DataFormat.objects.get(name='parquet'), content_object=self.subject, revision=revision
                )
                is_new = True

        # Update fields with file info and move file to new location
        dset.version = VERSION
        dset.file_size = file_size
        dset.hash = file_hash or ''
        dset.created_by = LabMember.objects.get(username=user) if isinstance(user, str) else user
        dset.generating_software = 'ibllib ' + ibllib_version
        dset.content_object = self.subject
        if aggregate_hash is not None:
            dset.json = {**(dset.json or {}), 'aggregate_hash': aggregate_hash}
        logger.info(('Created' if is_new else 'Updated') + ' aggregate dataset with UUID %s', dset.pk)
        # Validate dataset
        dset.full_clean()
        dset.save()

        # Set default_dataset field of any other datasets to False
        (Dataset
         .objects
         .filter(
            name=f'_ibl_{alf_object}.table.pqt', collection=collection, default_dataset=True,
            dataset_type=DatasetType.objects.get(name=f'{alf_object}.table'), object_id=self.subject.id)
         .exclude(pk=dset.pk)
         .update(default_dataset=False))

        out_file = self.output_path.joinpath(collection)
        if dset.revision:
            out_file /= f'#{dset.revision.name}#'
            out_file.mkdir(exist_ok=True)
        out_file /= alfiles.add_uuid_string(f'_ibl_{alf_object}.table.pqt', dset.pk)
        if out_file.exists():
            logger.warning('Output file %s already exists, overwriting', out_file)
        logger.info('%s -> %s', file_path, out_file)
        out_file = file_path.replace(out_file)
        assert out_file.exists(), f'failed to move {file_path} to {out_file}'

        # Update file records
        rel_path = alfiles.remove_uuid_string(out_file.relative_to(self.output_path)).as_posix()
        for repo in map(lambda x: DataRepository.objects.get(name=x), ('aws_aggregates', 'flatiron_aggregates')):
            kwargs = {'exists': repo.name.startswith('flatiron')}
            r, r_is_new = FileRecord.objects.update_or_create(
                dataset=dset, data_repository=repo, relative_path=rel_path, defaults=kwargs, create_defaults=kwargs)
            logger.info(('Created' if r_is_new else 'Updated') + ' file record %s: exists=%s', repo.name, r.exists)

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

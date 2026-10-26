"""Import Django then create various DataNotices.

https://docs.google.com/presentation/d/100amT_ztTFzFL5960NCsLGO6jJTh5UmueGTXLHz0S9I/edit?slide=id.p1#slide=id.p1

Notices are matched by name so this script may be re-run to add datasets from new revisions.
"""
import django
from django.db import transaction
from django.db.models import Q
import re
from collections import defaultdict
from datetime import date
from textwrap import dedent
from packaging.version import Version, parse, InvalidVersion

django.setup()

from data.models import DataNotice, Dataset
from actions.models import Session
from subjects.models import Project

COMMIT = False  # Set to True to create the notices; otherwise only print what would be done
CHUNK_SIZE = 1000  # Number of sessions per dataset query, and of rows per insert

# Trials datasets containing the ITI, i.e. trial intervals and stim off times
TRIALS_DATASETS = (
    '_ibl_trials.intervals.npy', '_ibl_trials.intervals_bpod.npy',
    '_ibl_trials.stimOff_times.npy', '_ibl_trials.table.pqt'
)
RAW_DATASETS = ('_iblrig_taskData.raw.jsonable',)

ITI_PREAMBLE = """\
    Inter-trial interval (ITI) is defined as the closed-loop period of grey screen between stimulus offset and enforced quiescence.
    During this time there is no visual stimulus and the subject is free to move the wheel.

    The ITI can be loaded with [brainbox.io.one.load_iti](https://github.com/int-brain-lab/ibllib/blob/78e82df8a51de0be880ee4076d2bb093bbc1d2c1/brainbox/io/one.py#L719-L738):
    ```python
    iti = np.r_[(np.roll(trials['intervals'][:, 0], -1) - trials['stimOff_times'])[:-1], np.nan]
    ```

    ### Expected behaviour
    The ITI was intended to be an exactly 1 second period between each trial, as defined in the original IBL behaviour paper.
    The preliminary experiments implemented in MATLAB (prior to iblrig) were not affected by this issue.
"""

# This iblrig pilot was implemented with the correct ITIs
exclude_project = Project.objects.get(name='ibl_behaviour_pilot_matlabrig')


def iter_protocols(min_version, max_version, habituation=False):
    """Yield (session pk, start time, protocol number) for choice world protocols with min_version <= version < max_version.

    If habituation is True only habituationChoiceWorld protocols are returned, otherwise these are excluded.
    """
    sessions = Session.objects.exclude(projects=exclude_project).exclude(task_protocol__isnull=True)
    sessions = sessions.values_list('pk', 'start_time', 'task_protocol').distinct()
    for pk, start_time, task_protocol in sessions.iterator(chunk_size=CHUNK_SIZE):
        for i, proc in enumerate(task_protocol.split('/')):
            # e.g. _iblrig_tasks_trainingChoiceWorld8.12.0, _iblrig_tasks_ephysChoiceWorld6.4.2
            # Passive and spontaneous protocols have no ITI
            if 'ChoiceWorld' not in proc or 'passive' in proc.lower():
                continue
            if habituation != ('habituation' in proc.lower()):
                continue
            if not (m := re.search(r'\D+(\d+\..*)', proc)):
                continue
            try:
                version = parse(m.groups()[0])
            except InvalidVersion:
                continue
            if min_version <= version < max_version:
                yield pk, start_time, i


def find_datasets(protocols, trials=TRIALS_DATASETS, raw=RAW_DATASETS, keep=None):
    """Yield chunks of (dataset pk, session pk) for the given protocols. Includes all revisions.

    If keep is given, it is called with each dataset's version and created_datetime and only datasets for which
    it returns True are yielded.
    """
    by_index = defaultdict(list)  # session pks grouped by protocol number
    for pk, _, i in protocols:
        by_index[i].append(pk)
    for i, pks in by_index.items():
        # First protocol may be in alf + raw_behavior_data (v7) or alf/task_00 + raw_task_data_00 (v8)
        alf = ('alf', 'alf/task_00') if i == 0 else (f'alf/task_{i:02d}',)
        raw_data = ('raw_behavior_data', 'raw_task_data_00') if i == 0 else (f'raw_task_data_{i:02d}',)
        query = Q(collection__in=alf, name__in=trials) | Q(collection__in=raw_data, name__in=raw)
        for j in range(0, len(pks), CHUNK_SIZE):
            # NB: the base manager avoids the dataset type / data format select related
            dsets = Dataset._base_manager.filter(query, session__in=pks[j:j + CHUNK_SIZE])
            rows = dsets.values_list('pk', 'session', 'version', 'created_datetime')
            yield [(pk, session) for pk, session, *extra in rows if keep is None or keep(*extra)]


def create_notice(protocols, ongoing=False, trials=TRIALS_DATASETS, raw=RAW_DATASETS, keep=None, **kwargs):
    """Create or update a notice by name and link its datasets in chunks.

    Each chunk is committed separately; existing links are ignored so an interrupted run may simply be re-run.
    The affected date range is derived from the sessions with affected datasets.
    """
    protocols = list(protocols)
    start_times = {pk: start_time for pk, start_time, _ in protocols}
    notice = None
    if COMMIT:
        notice, _ = DataNotice.objects.update_or_create(name=kwargs['name'], defaults=kwargs)
    Link = DataNotice.datasets.through
    sessions, n_datasets = set(), 0
    for rows in find_datasets(protocols, trials, raw, keep):
        sessions.update(session for _, session in rows)
        n_datasets += len(rows)
        if notice and rows:
            with transaction.atomic():
                links = [Link(datanotice_id=notice.pk, dataset_id=pk) for pk, _ in rows]
                Link.objects.bulk_create(links, batch_size=CHUNK_SIZE, ignore_conflicts=True)

    dates = sorted(start_times[pk] for pk in sessions)
    kwargs.setdefault('affected_date_start', dates[0].date() if dates else None)
    kwargs.setdefault('affected_date_end', None if ongoing or not dates else dates[-1].date())
    print(f'{kwargs["name"]!r}: {len(sessions)} sessions, {n_datasets} datasets, '
          f'{kwargs["affected_date_start"]} - {kwargs["affected_date_end"]}')
    if notice:
        DataNotice.objects.filter(pk=notice.pk).update(
            affected_date_start=kwargs['affected_date_start'], affected_date_end=kwargs['affected_date_end'])
    return notice


# Gradually increasing ITI (iblrig < v8)
create_notice(
    iter_protocols(Version('1'), Version('8')),
    name='Task ITI gradually increases',
    importance=DataNotice.IMPORTANCE.MINOR,
    description=dedent(ITI_PREAMBLE) + dedent("""\

        ### The cause
        The ITI was implemented in two stages: an `iti` state of 0.5 s within the Bpod state machine, followed by a
        'dead time' between state machines while the host computer prepared the next trial.
        Due to inefficient online behaviour plotting, this dead time grew over the course of a session, delaying the start
        of the next trial. The ITI started at around 1 s and increased to around 1.5 s after one hour, with occasional
        longer outliers.

        ### The fix
        In iblrig v8 the online plotting was improved and a timer was added to wait until the rest of the ITI duration
        before starting the next trial, making the ITI consistent across the session. This introduced a different issue;
        see the notice 'Incorrect ITI (1.5s)'.
    """),
    version_affected='iblrig v1 - v7',
)


# Constant 1.5 s ITI (iblrig v8.0.0 - v8.15.5)
create_notice(
    iter_protocols(Version('8'), Version('8.15.6')),
    name='Incorrect ITI (1.5s)',
    importance=DataNotice.IMPORTANCE.MINOR,
    description=dedent(ITI_PREAMBLE) + dedent("""\

        ### The cause
        The ITI was implemented in two distinct stages. The final `exit_state` of the Bpod state machine lasted
        `ITI_DELAY_SECS` = 1 s, then in between trials the host computer would
        [sleep until](https://github.com/int-brain-lab/iblrig/blob/8.15.5/iblrig/base_choice_world.py#L180-L183)
        `ITI_DELAY_SECS - 0.5` s had elapsed since the end of the previous state machine. This added a further 0.5 s,
        resulting in a consistent ITI of ~1.5 s.

        For imagingChoiceWorld, `ITI_DELAY_SECS` was 1.5 s, resulting in an ITI of ~2.5 s.

        ### The fix
        In iblrig v8.15.6, `ITI_DELAY_SECS` was reduced to 0.5 s with the intention of targeting a 1 s ITI
        ([commit](https://github.com/int-brain-lab/iblrig/commit/7d6105e686a1207bc401e149713f43e7028070f8)).
        Due to a further bug the ITI from this version onwards is ~0.5 s; see the notice 'Incorrect ITI (0.5s)'.

        In the same release, the `correct` state timer was changed to start from the beginning of the reward state,
        rather than the end, so stimulus offset on correct trials is earlier by the valve open time (~50 - 300 ms).
    """),
    version_affected='iblrig v8.0.0 - v8.15.5',
)


# Constant 0.5 s ITI (iblrig v8.15.6 onwards)
create_notice(
    iter_protocols(Version('8.15.6'), Version('9999')),
    ongoing=True,
    name='Incorrect ITI (0.5s)',
    importance=DataNotice.IMPORTANCE.MINOR,
    description=dedent(ITI_PREAMBLE) + dedent("""\

        ### The cause
        From iblrig v8.15.6, the final `exit_state` lasts `ITI_DELAY_SECS` = 0.5 s and the host computer was intended to
        wait a further `DEAD_TIME` = 0.5 s before starting the next trial. However, the dead time is
        [subtracted rather than added](https://github.com/int-brain-lab/iblrig/blob/8.29.5/iblrig/base_choice_world.py#L282-L288):
        ```python
        dt = self.task_params.ITI_DELAY_SECS - dead_time - (time.time() - time_last_trial_end)
        ```
        As `ITI_DELAY_SECS - DEAD_TIME` = 0, the host never waits and the ITI is ~0.5 s plus a small host overhead
        (typically < 50 ms).

        For imagingChoiceWorld, `ITI_DELAY_SECS` is 1.5 s, resulting in an ITI of ~2.5 s.

        ### The fix
        This issue is not yet fixed. A fix is expected in iblrig v8.30.0.
    """),
    version_affected='iblrig >= v8.15.6',
)


# Habituation trials extracted with ibllib < 2.27.0
# NB: This is an extraction issue so new revisions should not be added to this notice
HABITUATION_FIX_VERSION, HABITUATION_FIX_DATE = Version('2.27.0'), date(2023, 12, 14)
HABITUATION_DATASETS = (
    '_ibl_trials.intervals.npy', '_ibl_trials.intervals_bpod.npy',
    '_ibl_trials.itiIn_times.npy', '_ibl_trials.table.pqt'
)


def extracted_before_fix(version, created_datetime):
    """Return True if the dataset was extracted with ibllib < 2.27.0, falling back on the creation date."""
    try:
        return parse(re.sub(r'^\D+', '', version)) < HABITUATION_FIX_VERSION
    except InvalidVersion:
        return created_datetime.date() < HABITUATION_FIX_DATE


create_notice(
    iter_protocols(Version('1'), Version('9999'), habituation=True),
    trials=HABITUATION_DATASETS, raw=(), keep=extracted_before_fix,
    name='Incorrect habituation trial intervals',
    importance=DataNotice.IMPORTANCE.MINOR,
    description=dedent("""\
        The habituationChoiceWorld Bpod state machine has misleading state names. The `trial_start` state (`iti` from
        iblrig v8.17.0) is in fact the 1 s period of grey screen at the start of each trial, while the `iti` state
        (`post_reward` from iblrig v8.17.0) is the period after reward during which the stimulus remains in the centre
        of the screen.

        ### Expected behaviour
        A trial starts at stimulus onset and ends at stimulus offset. The `itiIn_times` mark the start of the grey screen period.

        ### The cause
        Prior to ibllib v2.27.0, habituation trials were extracted with the trainingChoiceWorld extractors, which took the
        state names at face value:

        - `intervals[:, 0]` was the start of the grey screen period, i.e. the stimulus offset of the previous trial.
        - `itiIn_times` was the time of reward offset, while the stimulus was still on screen.

        ### The fix
        From ibllib v2.27.0 ([ibllib#673](https://github.com/int-brain-lab/ibllib/pull/673)) the trial start is taken as the
        end of the grey screen period and the trial end as the start of the next trial's grey screen period (or the stimulus
        offset, if later). Re-extracting the trials with ibllib >= 2.27.0 resolves this issue.
    """),
    version_affected='ibllib < 2.27.0',
)


# Habituation stimulus remains in centre for too long after reward (iblrig < v8.17.0)
create_notice(
    iter_protocols(Version('1'), Version('8.17.0'), habituation=True),
    name='Habituation stimulus duration after reward',
    importance=DataNotice.IMPORTANCE.MINOR,
    description=dedent("""\
        ### Expected behaviour
        In habituationChoiceWorld, the Gabor patch is presented at one of two random initial locations (+/- 35 degrees
        azimuth) for an average of 10 seconds. The Gabor patch then appears in the centre of the screen, and 500 ms later
        a 3 uL reward is given. The Gabor patch stays in the centre for another 500 ms (so for 1 second in total) before it
        disappears. A period of grey screen follows before the next Gabor patch is presented. This grey screen period is on
        average 1 second.

        The stimulus offset can be loaded from `trials['stimOff_times']` and the reward onset from `trials['feedback_times']`.

        ### The cause
        The state after reward (`iti`, later `post_reward`) was intended to end the trial 500 ms after reward onset.
        Instead its duration was set by the `ITI` / `ITI_DELAY_SECS` parameter, and the stimulus only disappears at the
        start of the next trial, i.e. after any delay on the host computer between trials. The time from reward onset to
        stimulus offset was therefore:

        - iblrig v1 - v7: ~1 s, plus the variable delay between trials while the host computer prepared the next trial
        - iblrig v8.0.0 - v8.15.5: ~1.5 s plus the valve open time (`ITI_DELAY_SECS` = 1 s, plus a 0.5 s wait between
          trials; see the notice 'Incorrect ITI (1.5s)')
        - iblrig v8.15.6 - v8.16.1: ~0.5 s plus the valve open time (`ITI_DELAY_SECS` = 0.5 s)

        The grey screen period between trials was 1 s throughout, as intended.

        ### The fix
        From iblrig v8.17.0 the valve open time is subtracted from the post-reward state, so the stimulus remains in the
        centre for 500 ms after reward onset (plus a small host overhead).
    """),
    version_affected='iblrig v1 - v8.16.1',
)

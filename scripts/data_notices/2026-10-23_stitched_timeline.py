"""Import Django then create a DataNotice for sessions with stitched Timeline recordings.

In a few mesoscope sessions Timeline was stopped and restarted part way through, producing multiple recordings
(raw_sync_data_00, raw_sync_data_01, ...). These were concatenated into a single raw_sync_data collection with
ibldevtools/miles/mesoscope_fails/2023-08-15_merge-mesoscope-sync.py before extraction. Only the merged
recording is registered to Alyx so the sessions are listed explicitly.

The notice is matched by name so this script may be re-run to add datasets from new sessions and revisions.
"""
import django
from django.db import transaction
from django.db.models import Q
from textwrap import dedent

django.setup()

from data.models import DataNotice, Dataset
from actions.models import Session

COMMIT = False  # Set to True to create the notice; otherwise only print what would be done

# Sessions whose Timeline DAQ data were stitched together from multiple recordings
SESSIONS = (
    ('SP035', '2023-02-23', 2),
    ('SP037', '2023-02-17', 2),
    ('SP037', '2023-02-21', 1),
    ('SP037', '2023-02-23', 1),
    ('SP037', '2023-02-24', 1),
    ('SP080', '2026-07-07', 1),
)
# The merged raw DAQ data and the wheel datasets extracted from the encoder counter channel
DAQ_DATASETS = ('_timeline_DAQdata.raw.npy', '_timeline_DAQdata.timestamps.npy', '_timeline_DAQdata.meta.json')
WHEEL_DATASETS = (
    '_ibl_wheel.position.npy', '_ibl_wheel.timestamps.npy',
    '_ibl_wheelMoves.intervals.npy', '_ibl_wheelMoves.peakAmplitude.npy'
)

DESCRIPTION = """\
    ### Expected behaviour
    Timeline records the DAQ inputs (rotary encoder, Bpod, frame times, audio, etc.) continuously throughout a
    session, providing a single clock to which all other data are aligned.

    ### The cause
    In these sessions the Timeline acquisition was stopped and restarted part way through, producing two recordings
    separated by a gap of one to several minutes. The recordings were concatenated into a single `raw_sync_data`
    collection, with the timestamps of the second recording offset by the time between the start of the two
    recordings. Consequently:

    - **There is no DAQ data during the gap.** Any events (e.g. imaging frames, Bpod and audio TTLs) that occurred
      while Timeline was stopped were not recorded. The gap can be found as the largest difference between
      consecutive `_timeline_DAQdata.timestamps` values.
    - **The wheel position is discontinuous across the gap.** The rotary encoder is recorded on a DAQ counter
      channel, which restarts from zero with each recording. The extracted `_ibl_wheel.position` therefore steps
      by up to several hundred radians at the start of the second recording, and the wheel movement detected around
      the gap (`_ibl_wheelMoves`) is spurious. The position within each recording is unaffected, but the movement
      of the wheel during the gap is unknown.

    These discontinuities fail the `_task_wheel_discontinuities` task QC check.

    ### The fix
    This is a data acquisition issue and can not be fully corrected. Analyses of wheel position should treat the
    data before and after the gap separately.
"""


def find_datasets(sessions):
    """Return (dataset pk, session pk) for the stitched DAQ data and wheel datasets of the given sessions.

    Includes all revisions.
    """
    query = Q(collection='raw_sync_data', name__in=DAQ_DATASETS) | Q(collection__regex=r'^alf(/task_\d{2})?$', name__in=WHEEL_DATASETS)
    # NB: the base manager avoids the dataset type / data format select related
    return list(Dataset._base_manager.filter(query, session__in=sessions).values_list('pk', 'session'))


def create_notice(session_refs, **kwargs):
    """Create or update a notice by name and link the datasets of the given sessions.

    Existing links are ignored so the script may simply be re-run.
    """
    sessions = {}
    for subject, date, number in session_refs:
        session = Session.objects.filter(subject__nickname=subject, start_time__date=date, number=number).first()
        if not session:
            print(f'session {subject}/{date}/{number:03d} not found')
            continue
        sessions[session.pk] = session.start_time
    rows = find_datasets(list(sessions))
    linked = {session for _, session in rows}
    for pk in set(sessions) - linked:
        print(f'session {pk} has no datasets to link')
    dates = sorted(sessions[pk].date() for pk in linked)
    kwargs.setdefault('affected_date_start', dates[0] if dates else None)
    kwargs.setdefault('affected_date_end', dates[-1] if dates else None)
    print(f'{kwargs["name"]!r}: {len(linked)} sessions, {len(rows)} datasets, '
          f'{kwargs["affected_date_start"]} - {kwargs["affected_date_end"]}')
    if not COMMIT:
        return
    with transaction.atomic():
        notice, _ = DataNotice.objects.update_or_create(name=kwargs['name'], defaults=kwargs)
        Link = DataNotice.datasets.through
        links = [Link(datanotice_id=notice.pk, dataset_id=pk) for pk, _ in rows]
        Link.objects.bulk_create(links, ignore_conflicts=True)
    return notice


create_notice(
    SESSIONS,
    name='Stitched Timeline recordings',
    importance=DataNotice.IMPORTANCE.MAJOR,
    description=dedent(DESCRIPTION),
    version_affected='',
)

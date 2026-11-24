"""Import Django then create a DataNotice for sessions without histology probe alignments.

The first sessions we will make public without histology probe alignments are from Nate Miska's
analysis of subject SWC_NM_102 where the mouse brain was lost during imaging.
Nate still included the data for some analyses, as exact targeting was not critical, and ephys
was sufficient to show what probe regions were within midbrain.
"""
import sys
from pathlib import Path
from textwrap import dedent

import django
from django.db import transaction
from django.db.models import Q
from one.alf.spec import QC

django.setup()

from data.models import DataNotice, Dataset
from actions.models import Session
from experiments.models import TrajectoryEstimate

# NB: under `manage.py shell` __file__ is that of manage.py
candidates = (Path(globals().get('__file__', '.')).resolve().parents[2], Path('/home/iblalyx'))
IBL_ALYX_ROOT = next((p for p in candidates if p.joinpath('releases', 'utils.py').exists()), None)
assert IBL_ALYX_ROOT, 'No IBL_ALYX_ROOT found, it is usually at the same directory level as the alyx repo'
sys.path.append(str(IBL_ALYX_ROOT.parent))

from iblalyx.releases.utils import DTYPES_RELEASE_EPHYS_ALL

COMMIT = False  # Set to True to create the notice; otherwise only print what would be done
subjects = ('SWC_NM_102',)
sessions = Session.objects.filter(subject__nickname__in=subjects, probe_insertion__isnull=False).distinct()
SESSIONS = dict(sessions.values_list('pk', 'start_time'))

# Histology track (50) and ephys aligned histology track (70) estimates would mean the probes could be located
trajectories = TrajectoryEstimate.objects.filter(probe_insertion__session__in=SESSIONS, provenance__gte=50)
assert not trajectories.exists(), f'{trajectories.count()} histology trajectories found for {subjects}'


def find_datasets(sessions):
    """Return (dataset pk, session pk) for all ephys datasets in the given sessions that are of a type we would normally release.

    Includes all revisions.
    """
    query = Q(dataset_type__name__in=DTYPES_RELEASE_EPHYS_ALL)
    # NB: the base manager avoids the dataset type / data format select related
    return list(Dataset._base_manager.filter(query, session__in=sessions).values_list('pk', 'session'))


DESCRIPTION = """\
    ### Expected behaviour
    After each experiment the brain is imaged and registered to a histological atlas. The probe tracks are then traced.
    The ephys data are then used to align the channels to the traced tracks, giving the brain region and Allen CCF
    coordinates of each channel (`channels.brainLocationIds_ccf_2017`, `channels.mlapdv` and `electrodeSites.*`).

    ### The issue
    The brain of these subjects could either not be histologically aligned, or the probe tracks were not traced.
    Consequently:

    - **There are no anatomical locations.** The channel brain regions and coordinates are absent and the probe
      insertions have no histology trajectory estimates.
    - **Clusters can not be assigned to brain regions** using the usual ONE/brainbox loaders.

    The spike sorting and raw ephys data are unaffected. These sessions were included in Miska et al. 2026 for
    analyses where exact targeting was not critical.

    ### The fix
    The histology can not be recovered. Analyses requiring brain regions or coordinates should exclude these
    insertions.
"""

def create_notice(sessions, **kwargs):
    """Create or update a notice by name and link the datasets of the given sessions.

    Existing links are ignored so the script may simply be re-run.
    """
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


def set_location_qc(sessions):
    """Set the QC of any location datasets in the given sessions to CRITICAL, as they can not be correct."""
    query = Q(name__contains='.brainLocationIds') | Q(name__contains='.mlapdv')
    dsets = Dataset._base_manager.filter(query, session__in=sessions).exclude(qc=QC.CRITICAL)
    print(f'{dsets.count()} location datasets to set to CRITICAL')
    if COMMIT:
        dsets.update(qc=QC.CRITICAL)


set_location_qc(list(SESSIONS))

create_notice(
    SESSIONS,
    name='Missing histology probe alignments',
    importance=DataNotice.IMPORTANCE.CRITICAL,
    description=dedent(DESCRIPTION),
    version_affected='',
)

"""
This script tags all datasets in the Brainwidemap tag and creates a parquet file with all datasets in the Brainwidemap tag.
Brainwidemap tag is a superset of all datasets in the BWM tags, plus all pawstates datasets for sessions in
2026_Q3_ACNEI_BWM_pawstates.txt, as part of the ACNEI course material.
"""
import sys
import os
from pathlib import Path

import pandas as pd
import django

if __name__ == '__main__' and not os.environ.get('DJANGO_SETTINGS_MODULE'):
    sys.path.insert(0, '.')
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'alyx.settings')
    django.setup()


from django.db.models import Q
from actions.models import Session
from data.models import Dataset, Tag, DatasetType
from experiments.models import ProbeInsertion

import alyx.base

try:
    RELEASES_DIR = Path(__file__).resolve().parent
    IBL_ALYX_ROOT = RELEASES_DIR.parent
except NameError:
    IBL_ALYX_ROOT = Path('/home/iblalyx')
    RELEASES_DIR = IBL_ALYX_ROOT.joinpath('releases')
assert IBL_ALYX_ROOT.exists(), 'No IBL_ALYX_ROOT found, it is usually at the same directory level as the alyx repo'
sys.path.append(str(IBL_ALYX_ROOT.parent))
import iblalyx.releases.utils

DRY_RUN = True
TAG_NAME = 'Brainwidemap'

#### Tag all datasets already tagged with a BWM tag

dsets = Dataset.objects.prefetch_related('tags').filter(tags__name__contains='BWM').values_list('id', flat=True)
# This is a posthoc script so we expect that all datasets with a BWM tag are already in the Brainwidemap tag
assert set(Tag.objects.get(name=TAG_NAME).datasets.values_list('id', flat=True)) >= set(dsets), 'Not all datasets with a BWM tag are in the Brainwidemap tag'
if DRY_RUN is False:
    tag, _ = Tag.objects.get_or_create(name=TAG_NAME)
    tag.datasets.add(*dsets)

#### Tag all pawstates datasets for sessions in 2026_Q3_ACNEI_BWM_pawstates.txt
eids = RELEASES_DIR.joinpath('2026_Q3_ACNEI_BWM_pawstates.txt').read_text().strip().splitlines()

dtype = DatasetType.objects.get(name='camera.pawstates')
dsets = Dataset.objects.filter(session__id__in=eids, dataset_type=dtype).values_list('id', flat=True)

# Assert all sessions exist and have at least one pawstates dataset
no_session = set(eids) - set(map(str, Session.objects.filter(id__in=eids).values_list('id', flat=True)))
assert not no_session, 'Sessions not found:\n' + '\n'.join(sorted(no_session))
no_pawstates = set(eids) - set(map(str, Dataset.objects.filter(id__in=dsets).values_list('session_id', flat=True)))
assert not no_pawstates, 'Sessions without pawstates:\n' + '\n'.join(sorted(no_pawstates))

# Assert that for each session and view with pawstates, the camera and wheel datasets are already public
required = ('camera.lightningPose', 'camera.times', 'wheel.position', 'wheel.timestamps')
missing = []
pawstates = Dataset.objects.filter(session__id__in=eids, dataset_type=dtype).values_list('session_id', 'name')
for eid, name in pawstates:
    view = name.split('.')[0].removeprefix('_ibl_').removesuffix('Camera')  # e.g. _ibl_leftCamera -> left
    if view not in ('left', 'right'):
        continue
    public = Dataset.objects.filter(session_id=eid, tags__public=True)
    for dtype_name in required:
        d = public.filter(dataset_type__name=dtype_name)
        if dtype_name.startswith('camera.'):
            d = d.filter(name__contains=f'{view}Camera.')
        if not d.exists():
            missing.append((str(eid), view, dtype_name))
assert not missing, 'Missing public datasets:\n' + '\n'.join(map(str, missing))

if DRY_RUN is False:
    tag, _ = Tag.objects.get_or_create(name=TAG_NAME)
    tag.datasets.add(*dsets)

#### Create a parquet file with all datasets in the Brainwidemap tag
dsets = Dataset.objects.filter(tags__name=TAG_NAME)
df_datasets = iblalyx.releases.utils.dset2df(dsets)
df_datasets.to_parquet(RELEASES_DIR.joinpath(f'{TAG_NAME}_datasets.pqt'))

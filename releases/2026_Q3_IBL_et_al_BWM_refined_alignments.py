"""
Data Release request link:
https://docs.google.com/document/d/1T18OoEl_06OZwOpDrWS4q8WhRs8gHCyAsShxYT3YKJM/edit?tab=t.0

"""

import sys
from pathlib import Path

from data.models import Dataset, Tag
import alyx.base

IBL_ALYX_ROOT = Path(alyx.base.__file__).parents[3].joinpath('iblalyx')
assert IBL_ALYX_ROOT.exists(), 'No IBL_ALYX_ROOT found, it is usually at the same directory level as the alyx repo'
sys.path.append(str(IBL_ALYX_ROOT.parent))

import iblalyx.releases.utils

DRY_RUN = True

TAG_NAME = '2026_Q3_IBL_et_al_BWM_refined_alignments'

pids = ['81ef1ec7-dbcc-410f-9bff-b21a62a909fd',
        'B543e81e-4c8f-415e-82ec-631b177d19d2']

revision = '2026-08-07'

dtypes = [
    'electrodeSites.mlapdv.npy',
    'electrodeSites.brainLocationIds_ccf_2017.npy',
    'channels.mlapdv.npy',
    'channels.brainLocationIds_ccf_2017.npy',
]

dsets = Dataset.objects.filter(name__in=dtypes, probe_insertion__in=pids, revision__name=revision, default_dataset=True)

df_datasets = iblalyx.releases.utils.dset2df(dsets)
df_datasets.to_parquet(IBL_ALYX_ROOT.joinpath('releases', f'{TAG_NAME}_datasets.pqt'))

if DRY_RUN is False:
    dsets2tag = Dataset.objects.filter(id__in=df_datasets['dataset_id'])
    tag, _ = Tag.objects.get_or_create(name=TAG_NAME, protected=True, public=True)
    tag.datasets.set(dsets2tag)

    # Also add them to the main BWM tag
    tag_bwm = Tag.objects.get(name='Brainwidemap')
    tag_bwm.datasets.add(*dsets2tag)
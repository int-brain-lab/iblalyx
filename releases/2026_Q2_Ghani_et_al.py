import sys
from pathlib import Path

import pandas as pd

from data.models import Dataset, Tag
import alyx.base

IBL_ALYX_ROOT = Path(alyx.base.__file__).parents[3].joinpath('iblalyx')
assert IBL_ALYX_ROOT.exists(), 'No IBL_ALYX_ROOT found, it is usually at the same directory level as the alyx repo'
sys.path.append(str(IBL_ALYX_ROOT.parent))

import iblalyx.releases.utils

TAG_NAME = '2026_Q2_Ghani_et_al'
DRY_RUN = True

sessions = pd.read_csv(IBL_ALYX_ROOT.joinpath('releases', './2026_Q2_Ghani_et_al_sessions.csv'))['eid'].values

dsets = Dataset.objects.filter(session__in=sessions,
                               dataset_type__name__in=iblalyx.releases.utils.DTYPES_RELEASE_BEHAVIOUR)
df_datasets = iblalyx.releases.utils.dset2df(dsets)
df_datasets.to_parquet(IBL_ALYX_ROOT.joinpath('releases', f'{TAG_NAME}_datasets.pqt'))

if DRY_RUN is False:
    dsets2tag = Dataset.objects.filter(id__in=df_datasets['dataset_id'])
    tag, _ = Tag.objects.get_or_create(name=TAG_NAME, protected=True, public=True)
    tag.datasets.set(dsets2tag)

from pathlib import Path
import sys

import pandas as pd
from data.models import Dataset, Tag
from actions.models import Session
import alyx.base

IBL_ALYX_ROOT = Path(alyx.base.__file__).parents[3].joinpath('iblalyx')
assert IBL_ALYX_ROOT.exists(), 'No IBL_ALYX_ROOT found, it is usually at the same directory level as the alyx repo'
sys.path.append(str(IBL_ALYX_ROOT.parent))

import iblalyx.releases.utils

DRY_RUN = True
TAG_NAME = '2025_Q3_Davatolhagh_et_al_autism'

eids = ['b2aa9c2d-524e-4966-840c-f10482ae2c1a',
        '71ceb3d4-ca68-4380-8fe7-9f63d26222f6',
        '8df7b200-e44c-4c67-82e9-2666ba05d649',
        '2dda7005-3392-4de5-bd65-f90263d8229f',
        '2844dbf8-db2d-49ab-a5ba-490fb18c60fe',
        'c66ac898-82e5-4f37-826e-1e2cbd29c0f8',
        'eaa3be3b-49fc-4aa3-9abc-30b45db5cf4c',
        'ba9363ab-1d37-4f14-a158-85169d905c01',
        'a6dd4f2a-8e9f-4877-a5f7-09653ba30ac7',
        '088f44ce-926e-4a3a-808d-3f1e1a595c6f',
        'c4ef4d13-9a49-43f8-bd34-83262d9d1518',
        '81f90b18-e61c-4d32-bbce-3e0c5f33f06c',
        'e2946a6f-4157-4c38-ba7e-83c31c218ea7',
        'ba892860-149e-4bff-9961-aa6583d96661',
        'b052b9d7-3bfa-4d23-b195-99cfbd3f467c',
        '257ec2b8-6e8d-4b98-99de-a232b58fde2c',
        '76edf716-f3c5-4823-95f3-9d37ed9cbeae']

sess = Session.objects.filter(id__in=eids)
df_datasets = []

# Trials and wheel datasets
dsets = Dataset.objects.filter(session__in=sess, dataset_type__name__in=iblalyx.releases.utils.DTYPES_RELEASE_BEHAVIOUR)
df_datasets.append(pd.DataFrame([str(eid) for eid in dsets.values_list('pk', flat=True)], columns=['dataset_id']))

# Video datasets
dsets = iblalyx.releases.utils.get_video_datasets_for_ephys_sessions(eids, cam_labels=['left', 'right', 'body'])
df_datasets.append(pd.DataFrame([str(eid) for eid in dsets.values_list('pk', flat=True)], columns=['dataset_id']))

# Sync datasets
dsets = Dataset.objects.filter(session__in=sess, collection='raw_sync_data')
df_datasets.append(pd.DataFrame([str(eid) for eid in dsets.values_list('pk', flat=True)], columns=['dataset_id']))

# Raw widefield datasets
dsets = Dataset.objects.filter(session__in=sess, collection='raw_widefield_data')
df_datasets.append(pd.DataFrame([str(eid) for eid in dsets.values_list('pk', flat=True)], columns=['dataset_id']))

# Imaging datasets
dsets = Dataset.objects.filter(session__in=sess, name__icontains='imaging', collection='alf/widefield')
df_datasets.append(pd.DataFrame([str(eid) for eid in dsets.values_list('pk', flat=True)], columns=['dataset_id']))

# Concatenate datasets
df_datasets = pd.concat(df_datasets, axis=0)

if DRY_RUN is False:
    dsets2tag = Dataset.objects.filter(id__in=df_datasets['dataset_id'])
    tag, _ = Tag.objects.get_or_create(name=TAG_NAME, protected=True, public=True)
    tag.datasets.set(dsets2tag)

    df_datasets.to_parquer(IBL_ALYX_ROOT.joinpath(f'releases/{TAG_NAME}_datasets.pqt'))

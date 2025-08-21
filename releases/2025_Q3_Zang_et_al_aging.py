"""
Data Release request link:
https://docs.google.com/document/d/1b2hhuUMBXV8-yUDKmDznZmW8EmHroxf0AZ6cjZNOgJM/edit?tab=t.0
"""
# %%
import sys
from pathlib import Path

import pandas as pd
from django.db.models import Q

from data.models import Dataset, Tag
from experiments.models import ProbeInsertion
from actions.models import Session
import alyx.base

IBL_ALYX_ROOT = Path(alyx.base.__file__).parents[3].joinpath('iblalyx')
assert IBL_ALYX_ROOT.exists(), 'No IBL_ALYX_ROOT found, it is usually at the same directory level as the alyx repo'
sys.path.append(str(IBL_ALYX_ROOT.parent))

import iblalyx.releases.utils

pids = ['aebf71d2-408d-4901-a4e3-007cf61af8a0', '3ae57f3f-be93-48d9-8f47-f5a6f9055a3a', '34d7572f-7e3b-4d12-afc5-c26bd2dfde15', 'fa0dc413-1525-4178-89a8-3a093e08e2ce', '14e496f3-06c7-465e-b7a7-109d866793a2', '96bfe790-37a6-45da-aa30-e271c9ce68e6', '35e215e1-b925-4593-8d67-0de6603c525a', 'c7b21911-7699-402c-badc-670f723a7e42', '58153f9d-08b5-4386-b1ad-715c3922a470', '7d5c4c4e-5c23-4a1d-a1ce-32cb79bbcc2a', '2631e77a-d521-4939-b247-e7a0ea0a95c1', 'bf8bb47d-e7e4-4759-87f1-fe054407661b', '32622302-ffef-412b-8773-4c7ef2a993bf', '42fd1bb1-76b1-4227-9a74-925bcd28b9a0', '383e12dc-4825-46f4-ac1d-8f3f6a6a1381', 'cbe746bb-8076-41c9-a90e-3bd56b2d958e', '3b7827f2-3957-41dd-8b12-fa529a1422e7', '278ce01a-2383-4a09-b74a-b8571ccbabad', '5ce945a6-5b59-4d30-8be9-51f6e8280b43', '85b98361-9706-4318-8923-6988d4e804e8', 'ba40eda8-601d-41cb-a629-290d17e7a680', '23e12f60-05fc-43ce-9536-f2feef8db037', '39e6c9a9-2241-4781-9ed6-db45979207e7', '3d9db0eb-31a6-44a8-99de-6e04555d27be', '797fd358-5778-4b9f-b037-2aeb2393b839', 'd2da187c-7277-4114-bd09-f4f62ce9947e', 'e096bf1f-b2b7-471d-b07b-e4e6d65299ac', '08c49305-d12c-4cc5-8f5c-b29f62f3a4a6', '7246e8f8-f694-488f-8ce8-5214975ffe9a', '646bfb77-b784-4c21-b37f-2ffc9986a228', 'f8965106-8f4a-4910-81f6-f19d55878b4e', '9341b8f3-fa57-4fbb-9d0b-7ca3613da0cc', 'bb4fbbf0-4d1e-4d0d-b348-2d7b7fddd151', '18e665b6-cc3d-4cde-980b-ddba405c1b26', '4b345c19-4973-4f30-8858-f236e7456553', '770470e7-9e8c-40c0-b95b-33330c096ade', 'b9292b9f-cc04-4d2b-93c0-c0ad16e2b221', '6c74c0f6-030d-4665-9e2e-799b1bcd3367', 'becce8b9-db96-4ace-ad99-66397ca9e181', 'a7919b08-68fd-4ae7-a5a6-341d054d5bed', 'e8f25a3a-ab3d-4e3e-a863-5014a2b7e440', '556b57db-e4bd-456b-b0bf-d0ddc56603ff', '68386da9-4a3e-425f-baa1-15e4f985153d', '253768e5-b649-4e49-944d-cc79d30b8f35', '3c89bf07-1010-4457-8018-1733d50725e7', '488d4bf8-9ae6-4bbe-9025-fcfdaec93efd', '7f788d36-56dd-4ebc-863e-c22ec4f1a731', '429747bb-93c2-4ec2-b823-a49d9247d4d5', '9b52d705-f975-4efb-863f-8c0ee33495c9', 'eb48b9fc-661e-4dde-8388-f32bde00482f', '4dd3d15f-b59b-4403-85c0-417d41337f5d', '7215243f-f336-4602-9521-6f9786d4decd', '8f4213bd-49ab-4421-bf1b-a1f8e6e1f37f', '02106173-e888-429b-a03c-7daaa40cc6be', '7ca1a6a0-5808-4882-9f8c-ea5ae82ad1a2', 'fc93dc34-3329-475b-b779-1167482c86b7', '0bc568eb-feed-4d5d-86e2-edfdf84ed707', 'feb4c65d-ad57-430e-8698-31f193013d19', '3adebebc-6499-4ad7-81db-a7a61f50fb15', '442d6f32-f0dc-4f82-90e3-5eefb086797c', 'c52c4943-e764-4a9d-a759-06aff36993f0', 'd7c474c8-168a-4ae3-a2d8-573ca8017708', '1edd8ae4-02bb-47ab-868c-1d5fad6256aa', 'a089219a-c836-470a-91c7-d65617bfb82a']

# %%
TAG_NAME = '2025_Q3_Zang_et_al_Aging'
DRY_RUN = True
insertions = ProbeInsertion.objects.filter(id__in=pids)
eids = list(insertions.values_list('session__id', flat=True).distinct())
sessions = Session.objects.filter(id__in=eids)
subjects = list(sessions.values_list('subject__nickname', flat=True).distinct())
df_datasets = []

# video datasets: we exlude QC critical datasets and include lick times
dsets = iblalyx.releases.utils.get_video_datasets_for_ephys_sessions(eids, cam_labels=['left', 'right', 'body'])
df_datasets.append(iblalyx.releases.utils.dset2df(dsets))

# behaviour and wheel datasets
dsets = Dataset.objects.filter(session__in=sessions, dataset_type__name__in=iblalyx.releases.utils.DTYPES_RELEASE_BEHAVIOUR)
df_datasets.append(iblalyx.releases.utils.dset2df(dsets))

# ephys datasets: we release only iblsorter datasets
for ins in insertions:
    dtypes_no_ss = iblalyx.releases.utils.DTYPES_RELEASE_HISTOLOGY + iblalyx.releases.utils.DTYPES_RELEASE_EPHYS_RAW
    dsets = Dataset.objects.filter(session=ins.session, dataset_type__name__in=dtypes_no_ss, collection__icontains=ins.name)
    df_datasets.append(iblalyx.releases.utils.dset2df(dsets))
    dsets = Dataset.objects.filter(session=ins.session, dataset_type__name__in=iblalyx.releases.utils.DTYPES_RELEASE_SPIKE_SORTING,
                                   collection__icontains=f'alf/{ins.name}/iblsorter')
    df_datasets.append(iblalyx.releases.utils.dset2df(dsets))


# also get the tables
for subject in subjects:
    dsets = Dataset.objects.filter(
        session__isnull=True,
        dataset_type__name__in=['subjectSessions.table', 'subjectTraining.table', 'subjectTrials.table'],
        collection='Subjects',
        file_records__relative_path__icontains='/SP054/'
    ).distinct()
    # print(subject, len(dsets), 'aggregate tables found')
    df_datasets.append(iblalyx.releases.utils.dset2df(dsets))


# finalize
df_datasets = pd.concat(df_datasets, axis=0).reset_index(drop=True)
df_datasets.to_parquet(IBL_ALYX_ROOT.joinpath('releases', f'{TAG_NAME}.pqt'))


# %% Tagging in production database
if DRY_RUN is False:
    dsets2tag = Dataset.objects.filter(id__in=df_datasets['dataset_id'])
    tag, _ = Tag.objects.get_or_create(name=TAG_NAME, protected=True, public=True)
    tag.datasets.set(dsets2tag)



# %% Eventually output a pivoted table to summarize datasets per session
df_datasets_all = df_datasets.copy()
df_datasets_all['path'] = df_datasets_all.apply(lambda x: f'{x.collection}/{x["dataset_type"]}', axis=1)
ddf = df_datasets_all.reset_index().pivot_table(index='path', columns='eid', values='dataset_id', aggfunc='count')
# ddf.to_csv(f'/home/olivier/scratch/{TAG_NAME}_datasets.csv')


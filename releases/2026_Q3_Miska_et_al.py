"""
https://github.com/njmiska/Miska_et_al_2026/commit/874f35f416a2c02f11d95a74eef8395a5cfee0ad
https://raw.githubusercontent.com/njmiska/Miska_et_al_2026/874f35f416a2c02f11d95a74eef8395a5cfee0ad/release_manifest/dataset_requests.csv

Nate subsequently provided proposed_datasets.csv (and the same IDs in proposed_dataset_ids.csv). Rather than tagging
these exact datasets, the sessions and insertions they cover are used to select the default behaviour datasets in
SESSION_REQUESTS and the ephys datasets in DTYPES_RELEASE_EPHYS_ALL.
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
from data.models import Dataset, Tag
from experiments.models import ProbeInsertion

import alyx.base

RELEASES_DIR = Path(__file__).resolve().parent
IBL_ALYX_ROOT = RELEASES_DIR.parent
# if not IBL_ALYX_ROOT.exists():
#     IBL_ALYX_ROOT = Path('/home/iblalyx')
#     RELEASES_DIR = IBL_ALYX_ROOT.joinpath('releases')
assert IBL_ALYX_ROOT.exists(), 'No IBL_ALYX_ROOT found, it is usually at the same directory level as the alyx repo'
sys.path.append(str(IBL_ALYX_ROOT.parent))
import iblalyx.releases.utils

DRY_RUN = True
TAG_NAME = '2026_Q3_Miska_et_al'
PROPOSED_DATASETS = RELEASES_DIR.joinpath('2026_Q3_Miska_et_al_proposed_datasets.csv')

# Session-level datasets, released for every session in the proposed datasets
SESSION_REQUESTS = {
    'trials': Q(name__startswith='_ibl_trials.'),
    'wheel': Q(name__regex=r'^_ibl_wheel(Moves)?\.'),
    '_ibl_laserStimulation.intervals.npy': Q(
        name='_ibl_laserStimulation.intervals.npy'),
    '_iblrig_taskData.raw*': Q(name__startswith='_iblrig_taskData.raw'),
}
# Insertion-level datasets, released for every ephys_bs insertion
EPHYS_DTYPES = iblalyx.releases.utils.DTYPES_RELEASE_EPHYS_ALL

# %% Load the proposed datasets
df_proposed = pd.read_csv(PROPOSED_DATASETS)
for col in ['dataset_id', 'eid', 'identifier']:
    df_proposed[col] = df_proposed[col].str.lower()
print(df_proposed.groupby('pipeline')[['eid', 'identifier']].nunique(), end='\n\n')

# %% Check the Alyx records: sessions exist and PIDs belong to the expected session
eids = df_proposed['eid'].unique()
missing_eids = set(eids) - set(map(str, Session.objects.filter(id__in=eids).values_list('id', flat=True)))
assert not missing_eids, f'{len(missing_eids)} sessions not found: {missing_eids}'

df_pids = (df_proposed.loc[df_proposed['pipeline'] == 'ephys_bs', ['identifier', 'eid']]
           .drop_duplicates().rename(columns={'identifier': 'pid'}))
columns = ['id', 'session', 'name']
df_insertions = pd.DataFrame(
    ProbeInsertion.objects.filter(id__in=df_pids['pid']).values_list(*columns),
    columns=['pid', 'eid_db', 'probe_name']).astype(str)
df_pids = df_pids.merge(df_insertions, on='pid', how='left')
mismatch = df_pids[df_pids['eid'] != df_pids['eid_db']]
assert mismatch.empty, f'PIDs missing or not matching proposed sessions:\n{mismatch}'

# %% Session-level datasets
columns = ['id', 'session', 'collection', 'name']
matched = []
for request, query in SESSION_REQUESTS.items():
    dsets = Dataset.objects.filter(query, session__in=eids, default_dataset=True)
    df = pd.DataFrame(dsets.values_list(*columns), columns=columns).astype(str)
    matched.append(df.assign(request=request))
    print(f'{request}: found in {df["session"].nunique()} of {len(eids)} sessions')

# %% Insertion-level datasets, from the spike sorting / raw ephys collections in the proposed datasets
for pid, eid, probe_name in df_pids[['pid', 'eid', 'probe_name']].itertuples(index=False):
    proposed = df_proposed.loc[(df_proposed['eid'] == eid) & (df_proposed['pipeline'] == 'ephys_bs'), 'collection']
    collections = {c for c in proposed.unique() if probe_name in c or c == 'raw_ephys_data'}
    if not any(c.startswith('alf/') for c in collections):
        print(f'No spike sorting proposed for {pid}')
    collections.add(f'alf/{probe_name}')  # histology
    dsets = Dataset.objects.filter(
        session=eid, default_dataset=True, dataset_type__name__in=EPHYS_DTYPES, collection__in=collections)
    df = pd.DataFrame(dsets.values_list(*columns), columns=columns).astype(str)
    matched.append(df.assign(request='ephys'))

df_matched = pd.concat(matched, ignore_index=True).rename(columns={'session': 'eid'}).drop_duplicates('id')

# %% Compare with the proposed datasets
proposed_ids, matched_ids = set(df_proposed['dataset_id']), set(df_matched['id'])
not_released = df_proposed[~df_proposed['dataset_id'].isin(matched_ids)]
print(f'\n{len(not_released)} proposed datasets not released:')
print(not_released.groupby(['pipeline', 'dataset_type']).size().to_string(), end='\n\n')
# These are mostly expected, e.g. from sessions in SESSION_REQUESTS without any proposed dataset of that type
dsets = Dataset.objects.filter(id__in=matched_ids - proposed_ids)
df_extra = pd.DataFrame(dsets.values_list('collection', 'dataset_type__name'), columns=['collection', 'dataset_type'])
print(f'{len(df_extra)} released datasets not proposed:')
print(df_extra.value_counts(['dataset_type', 'collection']).sort_index().to_string(), end='\n\n')

# %% Save the dataset IDs for release in the public database
dsets = Dataset.objects.filter(id__in=matched_ids)
df_datasets = iblalyx.releases.utils.dset2df(dsets)
print(f'{len(df_datasets)} datasets from {df_datasets["eid"].nunique()} sessions')
df_datasets.to_parquet(RELEASES_DIR.joinpath(f'{TAG_NAME}_datasets.pqt'))

# %% Tagging in production database
if DRY_RUN is False:
    tag, _ = Tag.objects.get_or_create(name=TAG_NAME, protected=True, public=True)
    tag.datasets.set(dsets)

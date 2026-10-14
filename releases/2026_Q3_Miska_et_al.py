"""
https://github.com/njmiska/Miska_et_al_2026/commit/874f35f416a2c02f11d95a74eef8395a5cfee0ad
https://raw.githubusercontent.com/njmiska/Miska_et_al_2026/874f35f416a2c02f11d95a74eef8395a5cfee0ad/release_manifest/dataset_requests.csv

The manifest lists the ALF objects / datasets requested per session (zapit and opto_behavior pipelines) and per
probe insertion (ephys_bs pipeline). No dataset IDs are provided, so the default datasets are released.
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
assert IBL_ALYX_ROOT.exists(), 'No IBL_ALYX_ROOT found, it is usually at the same directory level as the alyx repo'
sys.path.append(str(IBL_ALYX_ROOT.parent))
import iblalyx.releases.utils

DRY_RUN = True
TAG_NAME = '2026_Q3_Miska_et_al'
MANIFEST_URL = ('https://raw.githubusercontent.com/njmiska/Miska_et_al_2026/'
                '874f35f416a2c02f11d95a74eef8395a5cfee0ad/release_manifest/dataset_requests.csv')

# Session-level requests, keyed by the manifest's object_or_dataset column
SESSION_REQUESTS = {
    'trials': Q(name__startswith='_ibl_trials.'),
    'wheel': Q(name__regex=r'^_ibl_wheel(Moves)?\.'),
    '_ibl_laserStimulation.intervals.npy': Q(
        name='_ibl_laserStimulation.intervals.npy'),
    '_iblrig_taskData.raw*': Q(name__startswith='_iblrig_taskData.raw'),
}
# Insertion-level datasets to release, and the request each one fulfils (matched on dataset name)
EPHYS_DTYPES = iblalyx.releases.utils.DTYPES_RELEASE_SPIKE_SORTING + iblalyx.releases.utils.DTYPES_RELEASE_HISTOLOGY
SPIKE_SORTING_REQUESTS = {
    'spikes': r'^spikes\.',
    'clusters': r'^clusters\.',
    'channels': r'^(channels|electrodeSites)\.',
    'waveforms_or_templates': r'^(templates|waveforms|_phy_spikes_subset)\.',
}
# Same preference order as brainbox.io.one.SpikeSortingLoader
SORTERS = ('iblsorter', 'pykilosort', '')

# %% Load the dataset requests
df_requests = pd.read_csv(MANIFEST_URL)
# df_requests.to_csv(RELEASES_DIR.joinpath(f'{TAG_NAME}_dataset_requests.csv'), index=False)
df_requests['eid'] = df_requests['eid'].str.lower()
df_requests['pid'] = df_requests['pid'].str.lower()
print(df_requests.groupby(['pipeline', 'request_kind', 'object_or_dataset']).size(), end='\n\n')

unknown = set(df_requests['object_or_dataset']) - set(SESSION_REQUESTS) - set(SPIKE_SORTING_REQUESTS)
assert unknown <= {'session', 'probe_insertion_and_session'}, f'Unhandled requests: {unknown}'

# %% Check the Alyx records: sessions exist and PIDs map to the expected session and probe name
eids = df_requests['eid'].unique()
missing_eids = set(eids) - set(map(str, Session.objects.filter(id__in=eids).values_list('id', flat=True)))
assert not missing_eids, f'{len(missing_eids)} sessions not found: {missing_eids}'

df_pids = df_requests.loc[df_requests['identifier_type'] == 'pid', ['pid', 'eid', 'probe_name']].drop_duplicates()
columns = ['id', 'session', 'name']
df_insertions = pd.DataFrame(
    ProbeInsertion.objects.filter(id__in=df_pids['pid']).values_list(*columns),
    columns=['pid', 'eid_db', 'probe_name_db']).astype(str)
df_pids = df_pids.merge(df_insertions, on='pid', how='left')
mismatch = df_pids[(df_pids['eid'] != df_pids['eid_db']) | (df_pids['probe_name'] != df_pids['probe_name_db'])]
assert mismatch.empty, f'PIDs missing or not matching manifest:\n{mismatch}'

# %% Session-level datasets
columns = ['id', 'session', 'collection', 'name']
matched = []
for request, query in SESSION_REQUESTS.items():
    request_eids = df_requests.loc[df_requests['object_or_dataset'] == request, 'eid'].unique()
    dsets = Dataset.objects.filter(query, session__in=request_eids, default_dataset=True)
    df = pd.DataFrame(dsets.values_list(*columns), columns=columns).astype(str)
    matched.append(df.assign(object_or_dataset=request, pid=None))

# %% Insertion-level datasets, from the collection SpikeSortingLoader would use
for pid, eid, probe_name in df_pids[['pid', 'eid', 'probe_name']].itertuples(index=False):
    dsets = Dataset.objects.filter(
        session=eid, default_dataset=True, dataset_type__name__in=EPHYS_DTYPES,
        collection__regex=rf'^(alf/{probe_name}(/[^/]+)?|raw_ephys_data(/{probe_name})?)$')
    df = pd.DataFrame(dsets.values_list(*columns), columns=columns).astype(str)
    sorter_collections = [f'alf/{probe_name}/{s}'.rstrip('/') for s in SORTERS]
    collection = next((c for c in sorter_collections if c in df['collection'].values), None)
    if collection is None:
        print(f'No spike sorting found for {pid}')
        continue
    # Histology is in the probe collection and sync in the raw ephys collections
    df = df[df['collection'].isin({collection, f'alf/{probe_name}', 'raw_ephys_data', f'raw_ephys_data/{probe_name}'})]
    # Label datasets with the request they fulfil; unlabelled ones (e.g. sync) are released regardless
    df = df.assign(object_or_dataset=None, pid=pid)
    for request, pattern in SPIKE_SORTING_REQUESTS.items():
        df.loc[df['name'].str.match(pattern), 'object_or_dataset'] = request
    matched.append(df)

df_matched = pd.concat(matched, ignore_index=True).rename(columns={'session': 'eid'})

# %% Report requests without any matching dataset
# Spike sorting requests are per insertion, all others per session (including those from ephys_bs PIDs)
is_ss = df_requests['object_or_dataset'].isin(SPIKE_SORTING_REQUESTS)
df_requests['key'] = df_requests['pid'].where(is_ss, df_requests['eid'])
df_matched['key'] = df_matched['pid'].fillna(df_matched['eid'])
found = set(df_matched[['key', 'object_or_dataset']].itertuples(index=False, name=None))
is_dataset_request = ~df_requests['request_kind'].eq('alyx_record')
is_found = df_requests[['key', 'object_or_dataset']].apply(tuple, axis=1).isin(found)
df_missing = df_requests[is_dataset_request & ~is_found]
print(df_missing.groupby(['required', 'pipeline', 'object_or_dataset']).size().rename('n_missing'), end='\n\n')
if df_missing['required'].any():
    print(f'WARNING: {df_missing["required"].sum()} required requests have no matching dataset')
    df_missing.to_parquet(RELEASES_DIR.joinpath(f'{TAG_NAME}_missing_datasets.pqt'), index=False)

# %% Save the dataset IDs for release in the public database
dsets = Dataset.objects.filter(id__in=df_matched['id'].unique())
df_datasets = iblalyx.releases.utils.dset2df(dsets)
print(f'{len(df_datasets)} datasets from {df_datasets["eid"].nunique()} sessions')
df_datasets.to_parquet(RELEASES_DIR.joinpath(f'{TAG_NAME}_datasets.pqt'))

# %% Tagging in production database
if DRY_RUN is False:
    tag, _ = Tag.objects.get_or_create(name=TAG_NAME, protected=True, public=True)
    tag.datasets.set(dsets)

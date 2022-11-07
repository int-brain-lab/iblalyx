import pandas as pd
from data.models import Tag, Dataset
from experiments.models import ProbeInsertion

df = pd.read_csv('2022_Q4_IBL_et_al_BWM_eids_pids.csv', index_col=0)
eids = df['eid'].unique()
video_exclude = [
    'dd4da095-4a99-4bf3-9727-f735077dba66',
    'e5fae088-ed96-4d9b-82f9-dfd13c259d52',
    '4d8c7767-981c-4347-8e5e-5d5fffe38534',
    'cea755db-4eee-4138-bdd6-fc23a572f5a1',
    '19b44992-d527-4a12-8bda-aa11379cb08c',
    '8c2f7f4d-7346-42a4-a715-4d37a5208535',
    'f8041c1e-5ef4-4ae6-afec-ed82d7a74dc1',
    'c728f6fd-58e2-448d-aefb-a72c637b604c',
    'af55d16f-0e31-4073-bdb5-26da54914aa2',
    'd832d9f7-c96a-4f63-8921-516ba4a7b61f',
    'dcceebe5-4589-44df-a1c1-9fa33e779727',
    '65f5c9b4-4440-48b9-b914-c593a5184a18',
    '4ddb8a95-788b-48d0-8a0a-66c7c796da96',
    'fa8ad50d-76f2-45fa-a52f-08fe3d942345',
    '8a3a0197-b40a-449f-be55-c00b23253bbf',
    '5455a21c-1be7-4cae-ae8e-8853a8d5f55e',
    '0c828385-6dd6-4842-a702-c5075f5f5e81',
    '19e66dc9-bf9f-430b-9d6a-acfa85de6fb7',
    '1e45d992-c356-40e1-9be1-a506d944896f',
]

# Trials data
trials_dtypes = ['trials.goCueTrigger_times', 'trials.stimOff_times', 'trials.table']
trials_dsets = Dataset.objects.filter(session__in=eids, collection='alf', dataset_types__in=trials_dtypes)

# Wheel data
wheel_dtypes = ['wheelMoves.intervals', 'wheelMoves.peakAmplitude', 'wheel.position', 'wheel.timestamps']
wheel_dsets = Dataset.objects.filter(session__in=eids, collection='alf', dataset_types__in=wheel_dtypes)

# Video data (some sessions excluded)
video_eids = [eid for eid in eids if eid not in video_exclude]
video_dtypes = ['camera.times', 'camera.dlc', 'camera.features', 'licks.times',
                'ROIMotionEnergy.position', 'camera.ROIMotionEnergy']
alf_video_dsets = Dataset.objects.filter(session__in=video_eids, collection='alf', dataset_types__in=video_dtypes)
raw_video_dsets = Dataset.objects.filter(session__in=video_eids, collection='raw_video_data', name__icontains='mp4')

probe_descr_dsets = Dataset.objects.filter(session__in=eids, collection='alf', dataset_type='probes.description')

# Probe related data
all_probes_dsets = []
for eid, probe_name in zip(df['eid'], df['probe_name']):

    collection = f'alf/{probe_name}'
    include = ['electrodeSites.brainLocationIds_ccf_2017.npy', 'electrodeSites.localCoordinates.npy',
               'electrodeSites.mlapdv.npy']
    probe_dsets = Dataset.objects.filter(session=eid, collection=collection, name__in=include)

    collection = f'alf/{probe_name}/pykilosort'
    probe_dsets = probe_dsets | Dataset.objects.filter(session=eid, collection=collection)

    collection = f'raw_ephys_data'
    probe_dsets = probe_dsets | Dataset.objects.filter(session=eid, collection=collection)

    collection = f'raw_ephys_data/{probe_name}'
    probe_dsets = probe_dsets | Dataset.objects.filter(session=eid, collection=collection)

    all_probes_dsets.append(probe_dsets)

# Combine all and tag
dsets = trials_dsets | wheel_dsets | alf_video_dsets | raw_video_dsets | probe_descr_dsets
for p in all_probes_dsets:
    dsets = dsets | p
tag, _ = Tag.objects.get_or_create(name="2022_Q4_IBL_et_al_BWM", protected=True, public=True)
tag.datasets.set(dsets)

# Save dataset IDs
dset_ids = [d.id for d in dsets]
df = pd.DataFrame(dset_ids, columns=['dataset_id'])
df.to_parquet('./2022_Q4_IBL_et_al_BWM_datasets.pqt')

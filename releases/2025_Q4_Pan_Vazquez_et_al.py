from data.models import Dataset, Tag
from experiments.models import ProbeInsertion
from actions.models import Session
import pandas as pd
import numpy as np

TAG_NAME = '2025_Q4_Pan_Vazquez_et_al'

pids = ['2e720a73-9f80-4871-95af-6812ac296097',
 '90726cf6-1324-4cfb-8b28-84f1f993b79c',
 '1078a062-bb28-478b-bc03-4c2c34f680d2',
 'ec0cd5ed-ef83-4f70-9832-111e1f9dede4',
 '71260fd9-a38a-46a3-8576-fee126671ac7',
 '945ce18d-f27c-43cb-9341-a079090dd6b9',
 '70ded061-967d-43d6-9723-6c0e5b8b24cf',
 'eeb69493-75f4-402d-ba6c-585a427eee7c',
 '7666a070-04a2-4cb9-b10c-37d4f78682d1',
 '72395e8e-b7bf-41ac-baac-bdc288129dc8',
 '3385b6fa-0eb1-428d-86f2-ec264fb2e234',
 '7abb636e-99a9-4170-9b35-2cc60cf57356',
 'ef40d113-6b5c-4456-ac3f-f9fe926e91b4',
 '28da0de8-46af-4567-af2a-729c3fed4d6c',
 '6a45ae7c-490a-4814-9d77-583b3be3958d',
 '76ea0435-033c-4612-9e41-3241d9f283cd',
 '47e735ca-f14a-44d3-bea0-0e6a5a771cd1',
 '9ae3fb24-9526-413f-bfb1-db9b29f88f24',
 '08f03147-6ef4-448d-9a6c-ba9ca21c29ef',
 '20152174-1b86-4a70-9440-71d731190619',
 'fca7eec0-d9fc-407a-8d5c-6e2e57de8c87',
 'd7616274-b0cf-464c-929c-54bb553f281b',
 '439e430e-3c58-4f0f-bd7e-73227bd88325',
 'fe942b24-4176-4c02-bf29-a1aa947275f5',
 '03d9a7d6-1aa6-4830-aa5d-7b917bec19f8',
 'c240cda2-f62d-4ec9-8aa8-d5fb086848e5',
 '314b0e5d-626e-445d-8916-fd0c2e0241f7',
 '5b50366b-4216-419e-be64-8de31d617d4b',
 '2389a4a9-2f77-4c43-8dac-347a9d098c46',
 '2fe2fb0a-af49-4279-a99e-d616b6a3c1da',
 '2cd00dbc-36f7-4a30-83d5-7a2d26451cc1',
 'b5f47989-27d7-4ac6-8ee7-a9985a34535f',
 '453c2957-b647-47ec-9c10-218e451b286b',
 '76814b88-62ba-4393-af4e-72657a49f92c',
 'e46b2ef7-2ece-4450-9052-12d031e05ab4',
 '42cfc6d1-68e2-4f46-8916-068c39d2a203',
 'ee2b3d0e-f38c-481b-82d9-eaf0983b8611',
 '45c3c3c6-e0b5-465b-9fb5-c6008cef2196',
 'd6e54c4f-2919-48a7-8eec-dfcd7845b37b',
 '46b0c6c9-927e-4d10-b554-0cbaae246417',
 'e23a517c-2947-48ab-91d2-d47239018962',
 '35e34c18-7dbd-4c5a-8318-bf89607f096f',
 'a73c3455-f049-4fc1-b685-edac84c7a36d',
 '1ffb7aa1-c5b9-4023-b367-a16284ebe096',
 '20840f25-817e-4b53-9334-ba489c388927',
 'a25932c7-a82c-4881-ac0a-7c5d9f19d2ce',
 '3dc959f7-6bc2-45db-9f8c-94a7e17f09ee',
 '4c640459-3c5d-43ad-9f36-208ad24f80f2',
 '73696336-a190-4008-a688-8b88df5b707a',
 '30924eb8-9285-4f2e-b5f7-308ae7b604a1',
 '9684a9a1-d3ac-4bb0-bf12-9912f2610adc',
 'e399a196-f0f3-4231-a196-0b59a3a3bcdf',
 '89a91bda-cce2-4a56-b226-0d56c8f50577',
 '04ff9db6-3a86-4b53-ad3f-09d921ac8d28',
 '438338dc-efe5-4c60-a20d-c1609180701e',
 'df79478e-f6b0-4105-9e75-deac5b8e2975',
 '0e5784e8-a136-48b4-b9dc-e750a1bbf15a',
 '510e989f-4004-4c78-8290-7728109eb063',
 'a73ed297-52ff-4796-af4c-c066f726e8de',
 '2a114fc4-af93-4f13-95bb-dc4178036da4',
 'b923a651-676f-4249-8880-df3aebbcb627',
 'e858736f-b855-472e-94c2-d9bc29c5ac6f',
 '140e2e30-68b2-4605-9a07-0da13acfe63c',
 'cb8a873e-491f-4eb5-89ac-1243a68ac1d8',
 '7018680e-6aeb-4d9f-ac8b-8364c4313ed0',
 'df8f2571-e9bb-46c8-badf-46444fb0b2d9',
 '415dc2f1-cdf9-4ed8-b117-1b409571ac39',
 '4f80dedb-12a8-43c2-a8a9-1e6ee98a8b5c',
 'a6b3ed73-9ee2-40d5-956b-76194d9a482f',
 '7803d888-542c-44a2-807a-ff670b219caf',
 '48f5b0ff-2823-46d4-ac23-8ef811f2653a',
 'b51eb38c-3bca-4231-8eb6-67647967b704',
 '32f56958-f00c-495b-8495-3e23a59c37bc',
 'becb8505-9bfc-4c03-841f-7c3ba277ed61',
 'bded3a59-f767-4d9b-b973-54ec925e232d',
 '54a67de9-3a7c-4667-b0c4-95b7804d39c6',
 'd212895f-e04d-4931-b878-b2e3212c54c3',
 'db84d53c-53da-4e54-b002-3372b6f74545',
 '04ee67c1-973a-4637-b82c-26c0bba541cd',
 '948cc48b-9ca2-42aa-b640-112ed9138e3d',
 '1d79b58a-3381-49a1-9a95-a0be4269c0e1',
 'b38313d4-4cc8-40a4-a5e8-e226e9098c05',
 '40dfa6e7-fd26-4722-9d9d-ea53a06bd2e8',
 'c8916e00-baa4-4ac9-8fdb-9ec4f47a9876',
 '94943c9f-f0b6-4cea-ad1f-33229ec477ff',
 'f8b8d6a7-673c-4bdc-b69e-098cac502b0c',
 'bac8641d-4009-4efb-abf4-90f68d61c6df',
 '3e75abf3-9236-4a5b-a7a0-a83e75c2f25b']


probes = ProbeInsertion.objects.filter(id__in=pids)
sessions = probes.values_list('session', flat=True)
sessions = Session.objects.filter(id__in=sessions)

# SESSION BASED
# Video data # TODO should we release this?
vid_dtypes = ['camera.times']
video_dsets = Dataset.objects.none()
for sess in sessions:
    for cam in ['left', 'right', 'body']:
        video_dsets = video_dsets | Dataset.objects.filter(session=sess,
                                                           name__icontains=cam,
                                                           dataset_type__name__in=vid_dtypes,
                                                           default_dataset=True)
        video_dsets = video_dsets | Dataset.objects.filter(session=sess, default_dataset=True,
                                                           name=f'_iblrig_{cam}Camera.raw.mp4')

dsets, counts = np.unique(video_dsets.values_list('name', flat=True), return_counts=True)
assert len(dsets) == 6
assert all(counts == sessions.count())


# Trials data ibl
trials_dtypes = [
    'trials.goCueTrigger_times',
    '_ibl_trials.stimOnTrigger_times',
    'trials.table',
    '_ibl_trials.laserStimulation']

trials_dsets = Dataset.objects.filter(session__in=sessions, dataset_type__name__in=trials_dtypes, default_dataset=True)
dsets, counts = np.unique(trials_dsets.values_list('name', flat=True), return_counts=True)
assert len(dsets) == len(trials_dtypes)
assert all(counts == sessions.count())

# Trials data av
av_trials_dtypes = [
    '_av_trials.feedbackType.npy',
    '_av_trials.laserOnset_times.npy',
    '_av_trials.laserProbability.npy',
    '_av_trials.leftReward.npy',
    '_av_trials.probabilityRewardLeft.npy',
    '_av_trials.rightReward.npy',
    '_av_trials.modelPredictions.pqt']

av_trials_dsets = Dataset.objects.filter(session__in=sessions, name__in=av_trials_dtypes, default_dataset=True)
dsets, counts = np.unique(av_trials_dsets.values_list('name', flat=True), return_counts=True)
assert len(dsets) == len(av_trials_dtypes)
assert all(counts == sessions.count())

# Wheel data
wheel_dtypes = ['wheelMoves.intervals', 'wheelMoves.peakAmplitude', 'wheel.position', 'wheel.timestamps']
wheel_dsets = Dataset.objects.filter(session__in=sessions, dataset_type__name__in=wheel_dtypes, default_dataset=True)
dsets, counts = np.unique(wheel_dsets.values_list('name', flat=True), return_counts=True)
assert len(dsets) == len(wheel_dtypes)
assert all(counts == sessions.count())

# Sync datasets
sync_dsets = Dataset.objects.filter(session__in=sessions, collection='raw_ephys_data', default_dataset=True)
expected = [
    'nidq.cbin',
    'nidq.ch',
    'nidq.meta',
    'sync.channels',
    'sync.times',
    'sync.polarities'
]
for exp in expected:
    dsets = sync_dsets.filter(name__icontains=exp)
    assert dsets.count() == sessions.count()
assert sync_dsets.count() == sessions.count() * len(expected)

# PROBE BASED
all_probes_dset_ids = []
for i, probe in enumerate(probes):
    collection = f'alf/{probe.name}'
    include = ['electrodeSites.brainLocationIds_ccf_2017.npy', 'electrodeSites.localCoordinates.npy',
               'electrodeSites.mlapdv.npy']
    probe_dsets = Dataset.objects.filter(session=probe.session, collection=collection, default_dataset=True,
                                         name__in=include)

    # The spikesorting data
    collection = f'alf/{probe.name}/pykilosort'
    probe_dsets = probe_dsets | Dataset.objects.filter(session=probe.session, collection=collection,
                                                       default_dataset=True)

    collection = f'raw_ephys_data/{probe.name}'
    probe_dsets = probe_dsets | Dataset.objects.filter(session=probe.session, collection=collection,
                                                       default_dataset=True)

    probe_dset_ids = probe_dsets.values_list('id', flat=True)
    all_probes_dset_ids.extend(probe_dset_ids)

all_probes_dsets = Dataset.objects.filter(id__in=all_probes_dset_ids)

all_probes_dsets = all_probes_dsets.exclude(name='_ibl_log.info_pykilosort.log')

# Check some raw data
expected = [
    'ap.cbin',
    'ap.meta',
    'lf.cbin',
    'lf.ch',
    'sync.channels',
    'sync.times',
    'sync.polarities'
]
for exp in expected:
    dsets = all_probes_dsets.filter(name__icontains=exp)
    assert dsets.count() == probes.count()

# Check some spike sorting and manual labels
expected = [
    'spikes.times',
    'templates.waveformsChannels',
    'electrodeSites.brainLocationIds_ccf_2017',
    '_av_clusters.channels',
    '_av_clusters.curatedLabels',
]
for exp in expected:
    dsets = all_probes_dsets.filter(name__icontains=exp)
    assert dsets.count() == probes.count()

# Check some manually curated (expected 60 probes)
expected = [
    '_av_spikes.clusters',
    '_av_clusters.waveformsChannels',
    '_av_clusters.metrics',
]

for exp in expected:
    dsets = all_probes_dsets.filter(name__icontains=exp)
    assert dsets.count() == 60

# Combine all datasets
new_dsets = trials_dsets | av_trials_dsets | wheel_dsets | video_dsets | sync_dsets | all_probes_dsets
new_dsets = new_dsets.distinct()

# Save the dataset ids
new_dids = [str(d.id) for d in new_dsets]
df = pd.DataFrame(new_dids, columns=['dataset_id'])
df.to_parquet(f'./{TAG_NAME}_datasets.pqt')

# Tag the datasets
tag, _ = Tag.objects.get_or_create(name=TAG_NAME, protected=True, public=True)
tag.datasets.set(new_dsets)
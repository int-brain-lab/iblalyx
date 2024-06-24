import pandas as pd

from data.models import Dataset, Tag
from experiments.models import ProbeInsertion
from actions.models import Session

tag_rep = '2022_Q2_IBL_et_al_RepeatedSite'

pids_to_release = [
    '0b8ea3ec-e75b-41a1-9442-64f5fbc11a5a',
    'e42e948c-3154-45cb-bf52-408b7cda0f2f',
    'ce397420-3cd2-4a55-8fd1-5e28321981f4',
    'e4ce2e94-6fb9-4afe-acbf-6f5a3498602e',
    'b83407f8-8220-46f9-9b90-a4c9f150c572',
    '92822789-608f-44a6-ad64-fe549402b2df',
    'f4bd76a6-66c9-41f3-9311-6962315f8fc8',
    '3d3d5a5e-df26-43ee-80b6-2d72d85668a5',
    'e31b4e39-e350-47a9-aca4-72496d99ff2a',
    '6fc4d73c-2071-43ec-a756-c6c6d8322c8b',
    '4836a465-c691-4852-a0b1-dcd2b1ce38a1',
    '1e176f17-d00f-49bb-87ff-26d237b525f1',
    '92033a0c-5a14-471b-b131-d43c72ca5d7a',
    '16799c7a-e395-435d-a4c4-a678007e1550',
    'b25799a5-09e8-4656-9c1b-44bc9cbb5279',
    'c17772a9-21b5-49df-ab31-3017addea12e',
    '22212d26-a167-45fb-9963-35ecd003e8a2',
    '0851db85-2889-4070-ac18-a40e8ebd96ba',
    'eeb27b45-5b85-4e5c-b6ff-f639ca5687de',
    '69f42a9c-095d-4a25-bca8-61a9869871d3',
    '8c732bf2-639d-496c-bf82-464bc9c2d54b',
    'f03b61b4-6b13-479d-940f-d1608eb275cc',
    '478de1ce-d7e7-4221-9365-2abdc6e88fb6',
    'f86e9571-63ff-4116-9c40-aa44d57d2da9',
    '31f3e083-a324-4b88-b0a4-7788ec37b191',
    'f2ee886d-5b9c-4d06-a9be-ee7ae8381114',
    'f26a6ab1-7e37-4f8d-bb50-295c056e1062',
    'f2a098e7-a67e-4125-92d8-36fc6b606c45',
    'c4f6665f-8be5-476b-a6e8-d81eeae9279d',
    '57656bee-e32e-4848-b924-0f6f18cfdfb1',
    '1a60a6e1-da99-4d4e-a734-39b1d4544fad',
    '27bac116-ea57-4512-ad35-714a62d259cd',
    '7d999a68-0215-4e45-8e6c-879c6ca2b771',
    '9117969a-3f0d-478b-ad75-98263e3bfacf',
    '6b6af675-e1ef-43a6-b408-95cfc71fe2cc',
    'febb430e-2d50-4f83-87a0-b5ffbb9a4943',
    'ad714133-1e03-4d3a-8427-33fc483daf1a',
    'd004f105-9886-4b83-a59a-f9173131a383',
    '4b93a168-0f3b-4124-88fa-a57046ca70e1',
    'bc1602ba-dd6c-4ae4-bcb2-4925e7c8632a',
    '7cbecb3f-6a8a-48e5-a3be-8f7a762b5a04',
    '17d9710a-f292-4226-b033-687d54b6545a',
    'c07d13ed-e387-4457-8e33-1d16aed3fd92',
    'a8a59fc3-a658-4db4-b5e8-09f1e4df03fd',
    '8b7c808f-763b-44c8-b273-63c6afbc6aae',
    'e49f221d-399d-4297-bb7d-2d23cc0e4acc',
    '80f6ffdd-f692-450f-ab19-cd6d45bfd73e',
    '63517fd4-ece1-49eb-9259-371dc30b1dd6',
    'f93bfce4-e814-4ae3-9cdf-59f4dcdedf51',
    '8ca1a850-26ef-42be-8b28-c2e2d12f06d6',
    '70da415f-444d-4148-ade7-a1f58a16fcf8',
    '6e1379e8-3af0-4fc5-8ba8-37d3bb02226b',
    '8d59da25-3a9c-44be-8b1a-e27cdd39ca34',
    'bf96f6d6-4726-4cfa-804a-bca8f9262721',
    'f68d9f26-ac40-4c67-9cbf-9ad1851292f7',
    'a3d13b05-bf4d-427a-a2d5-2fe050d603ec',
    '19baa84c-22a5-4589-9cbd-c23f111c054c',
    '143dd7cf-6a47-47a1-906d-927ad7fe9117',
    '84bb830f-b9ff-4e6b-9296-f458fb41d160',
    '730770d6-617a-4ada-95db-a48521befda5',
    '7a620688-66cb-44d3-b79b-ccac1c8ba23e',
    '02cc03e4-8015-4050-bb42-6c832091febb',
    'a12c8ae8-d5ad-4d15-b805-436ad23e5ad1',
    '523f8301-4f56-4faf-ab33-a9ff11331118',
    'b749446c-18e3-4987-820a-50649ab0f826',
    '36362f75-96d8-4ed4-a728-5e72284d0995',
    '8abf098f-d4f6-4957-9c0a-f53685db74cc',
    '84fd7fa3-6c2d-4233-b265-46a427d3d68d',
    '63a32e5c-f63a-450d-85cb-140947b67eaf',
    'ee3345e6-540d-4cea-9e4a-7f1b2fb9a4e4',
    'c6e294f7-5421-4697-8618-8ccc9b0269f6',
    '6d9b6393-6729-4a15-ad08-c6838842a074',
    '485b50c8-71e1-4654-9a07-64395c15f5ed',
    '3fded122-619c-4e65-aadd-d5420978d167',
    '9657af01-50bd-4120-8303-416ad9e24a51',
    'bbe6ebc1-d32f-42dd-a89c-211226737deb',
    'dab512bd-a02d-4c1f-8dbc-9155a163efc0',
    '1f5d62cb-814f-4ab2-b6af-7557ea04d56a',
    'f9d8aacd-b2a0-49f2-bd71-c2f5aadcfdd1',
    '0aafb6f1-6c10-4886-8f03-543988e02d9e',
    '94e948c1-f7be-4868-893a-f7cd2df3313e',
    'b2746c16-7152-45a3-a7f0-477985638638',
    '1f3d3fcb-f188-47a2-87e5-ac1db6cf393a',
    '7f3dddf8-637f-47bb-a7b7-e303277b2107',
    'ca5764ea-a57e-49de-8156-84da18ad439f',
    'ae252f7b-0224-4925-8174-7b25c2385bb7',
    'dc50c3de-5d84-4408-9725-22ae55b93522',
    '11a5a93e-58a9-4ed0-995e-52279ec16b98',
]

assert len(pids_to_release) == 88

probes = ProbeInsertion.objects.filter(id__in=pids_to_release)
sessions = probes.values_list('session', flat=True)
sessions = Session.objects.filter(id__in=sessions)

# Video data (some excluded)
vid_dtypes = ['camera.times', 'camera.dlc', 'camera.features', 'ROIMotionEnergy.position', 'camera.ROIMotionEnergy']
video_dsets = Dataset.objects.none()
for sess in sessions:
    for cam in ['left', 'right', 'body']:
        if sess.extended_qc.get(f'video{cam.capitalize()}', None) != 'CRITICAL':
            video_dsets = video_dsets | Dataset.objects.filter(session=sess,
                                                               name__icontains=cam,
                                                               dataset_type__name__in=vid_dtypes,
                                                               default_dataset=True)
            video_dsets = video_dsets | Dataset.objects.filter(session=sess, default_dataset=True,
                                                               name=f'_iblrig_{cam}Camera.raw.mp4')

# Lightening pose
lit_pose = Session.objects.filter(id__in=video_dsets.values_list('session_id', flat=True))
for l in lit_pose:
    if not (l.extended_qc.get('videoLeft', None) == 'CRITICAL'):
        video_dsets = video_dsets | Dataset.objects.filter(session=l, default_dataset=True,
                                                           dataset_type__name=f'camera.lightningPose')


# Licks times should be released if either left or right cam or both released
lick_sess = Session.objects.filter(id__in=video_dsets.values_list('session_id', flat=True))
for l in lick_sess:
    if not (l.extended_qc.get('videoLeft', None) == 'CRITICAL' and l.extended_qc.get('videoRight', None) == 'CRITICAL'):
        video_dsets = video_dsets | Dataset.objects.filter(session=l, default_dataset=True,
                                                           dataset_type__name=f'licks.times')

# Trials data
trials_dtypes = ['trials.goCueTrigger_times', 'trials.stimOff_times', 'trials.table']
trials_dsets = Dataset.objects.filter(session__in=sessions, dataset_type__name__in=trials_dtypes, default_dataset=True)

# Wheel data
wheel_dtypes = ['wheelMoves.intervals', 'wheelMoves.peakAmplitude', 'wheel.position', 'wheel.timestamps']
wheel_dsets = Dataset.objects.filter(session__in=sessions, dataset_type__name__in=wheel_dtypes, default_dataset=True)

# Probe description dataset (session level)
probe_descr_dsets = Dataset.objects.filter(session__in=sessions, collection='alf', default_dataset=True,
                                           dataset_type__name='probes.description')

# This is session level data, if another probe of this session has already been released, we don't add it
session_ephys_dsets = Dataset.objects.filter(session__in=sessions, collection='raw_ephys_data', default_dataset=True)

# Probe related data
all_probes_dset_ids = []
for i, probe in enumerate(probes):
    collection = f'alf/{probe.name}'
    include = ['electrodeSites.brainLocationIds_ccf_2017.npy', 'electrodeSites.localCoordinates.npy',
               'electrodeSites.mlapdv.npy']
    probe_dsets = Dataset.objects.filter(session=probe.session, collection=collection, default_dataset=True,
                                         name__in=include)

    # The original spike sorting (exclude new BWM spikesorting)
    collection = f'alf/{probe.name}/pykilosort'
    probe_dsets = probe_dsets | Dataset.objects.filter(session=probe.session, collection=collection,
                                                       default_dataset=True).exclude(revision__name='2024-05-06')

    # The repro ephys spike sorting (exclude new BWM spikesorting)
    collection = f'alf/{probe.name}/pykilosort'
    probe_dsets = probe_dsets | Dataset.objects.filter(session=probe.session, collection=collection,
                                                       revision__name='2024-03-22').exclude(revision__name='2024-05-06')

    collection = f'raw_ephys_data/{probe.name}'
    probe_dsets = probe_dsets | Dataset.objects.filter(session=probe.session, collection=collection,
                                                       default_dataset=True)

    probe_dset_ids = probe_dsets.values_list('id', flat=True)
    all_probes_dset_ids.extend(probe_dset_ids)

all_probes_dsets = Dataset.objects.filter(id__in=all_probes_dset_ids)

# Combine all datasets
new_dsets = trials_dsets | wheel_dsets | video_dsets | probe_descr_dsets | session_ephys_dsets | all_probes_dsets
new_dsets = new_dsets.distinct()

# Remove any datasets that were part of the previous repeated site tag
new_dsets = new_dsets.exclude(tags__name=tag_rep)

# Save the dataset ids
new_dids = [str(d.id) for d in new_dsets]
df = pd.DataFrame(new_dids, columns=['dataset_id'])
df.to_parquet('./2024_Q2_IBL_et_al_RepeatedSite_datasets.pqt')

# Tag the datasets
tag, _ = Tag.objects.get_or_create(name="2024_Q2_IBL_et_al_RepeatedSite", protected=True, public=True)
tag.datasets.set(new_dsets)


# Save the sessions and the insertions
df = pd.DataFrame(
    columns=['eid', 'pid', 'probe_name'],
    data=zip([str(p.session.id) for p in probes],
             [str(p.id) for p in probes],
             [str(p.name) for p in probes])
)
df.to_csv('./2024_Q2_IBL_et_al_RepeatedSite_eids_pids.csv')


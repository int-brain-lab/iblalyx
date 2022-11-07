import pandas as pd
from data.models import Tag, Dataset

df = pd.read_csv('2022_Q4_IBL_et_al_BWM_eids_pids.csv', index_col=0)
video_exclude = pd.read_csv('/home/ubuntu/paper-brain-wide-map/data_checks/video_exclude.csv', index_col=0)
video_exclude.columns = ['left', 'right', 'body']
eids = df['eid'].unique()


# Trials data
trials_dtypes = ['trials.goCueTrigger_times', 'trials.stimOff_times', 'trials.table']
trials_dsets = Dataset.objects.filter(session__in=eids, dataset_type__name__in=trials_dtypes, default_dataset=True)

# Wheel data
wheel_dtypes = ['wheelMoves.intervals', 'wheelMoves.peakAmplitude', 'wheel.position', 'wheel.timestamps']
wheel_dsets = Dataset.objects.filter(session__in=eids, dataset_type__name__in=wheel_dtypes, default_dataset=True)

# Video data (some excluded)
vid_eids = [eid for eid in eids if eid not in video_exclude.index]
vid_dtypes = ['camera.times', 'camera.dlc', 'camera.features', 'licks.times',
              'ROIMotionEnergy.position', 'camera.ROIMotionEnergy']
alf_video_dsets = Dataset.objects.filter(session__in=vid_eids, dataset_type__name__in=vid_dtypes, default_dataset=True)
raw_video_dsets = Dataset.objects.filter(session__in=vid_eids, default_dataset=True,
                                         name__in=['_iblrig_leftCamera.raw.mp4',
                                                   '_iblrig_rightCamera.raw.mp4',
                                                   '_iblrig_bodyCamera.raw.mp4'])
# For those that have only some videos excluded
for eid in video_exclude[~video_exclude.all(axis=1)].index:
    cams_include = [cam for cam in ['left', 'right', 'body'] if video_exclude.loc[eid, cam] == False]
    for cam in cams_include:
        alf_video_dsets = alf_video_dsets | Dataset.objects.filter(session=eid,
                                                                   name__icontains=cam,
                                                                   dataset_type__name__in=vid_dtypes,
                                                                   default_dataset=True)
        raw_video_dsets = raw_video_dsets | Dataset.objects.filter(session=eid, default_dataset=True,
                                                                   name=f'_iblrig_{cam}Camera.raw.mp4')
# Probe description dataset (session level)
probe_descr_dsets = Dataset.objects.filter(session__in=eids, collection='alf', default_dataset=True,
                                           dataset_type__name='probes.description')

# Probe related data
all_probes_dset_ids = []
for eid, probe_name in zip(df['eid'], df['probe_name']):

    collection = f'alf/{probe_name}'
    include = ['electrodeSites.brainLocationIds_ccf_2017.npy', 'electrodeSites.localCoordinates.npy',
               'electrodeSites.mlapdv.npy']
    probe_dsets = Dataset.objects.filter(session=eid, collection=collection, default_dataset=True, name__in=include)

    collection = f'alf/{probe_name}/pykilosort'
    probe_dsets = probe_dsets | Dataset.objects.filter(session=eid, collection=collection, default_dataset=True)

    collection = f'raw_ephys_data'
    probe_dsets = probe_dsets | Dataset.objects.filter(session=eid, collection=collection, default_dataset=True)

    collection = f'raw_ephys_data/{probe_name}'
    probe_dsets = probe_dsets | Dataset.objects.filter(session=eid, collection=collection,
                                                       default_dataset=True).exclude(name__icontains='ephysTimeRmsAP')

    probe_dset_ids = probe_dsets.values_list('id', flat=True)
    all_probes_dset_ids.extend(probe_dset_ids)

# Combine all and tag
all_probes_dsets = Dataset.objects.filter(id__in=all_probes_dset_ids)
dsets = trials_dsets | wheel_dsets | alf_video_dsets | raw_video_dsets | probe_descr_dsets | all_probes_dsets
dsets = dsets.distinct()

tag, _ = Tag.objects.get_or_create(name="2022_Q4_IBL_et_al_BWM", protected=True, public=True)
tag.datasets.set(dsets)

# Save dataset IDs
dset_ids = [str(d.id) for d in dsets]
df = pd.DataFrame(dset_ids, columns=['dataset_id'])
df.to_parquet('./2022_Q4_IBL_et_al_BWM_datasets.pqt')

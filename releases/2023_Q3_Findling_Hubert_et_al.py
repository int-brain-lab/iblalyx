import pandas as pd
from data.models import Tag, Dataset
from actions.models import Session


"""""""""
# OLD CODE
"""""""""

# wfield_dataset_types = [
#  'imaging.times.npy',
#  'imagingLightSource.properties.htsv',
#  'imaging.imagingLightSource.npy',
#  'widefieldLandmarks.dorsalCortex.json',
#  'widefieldSVT.haemoCorrected.npy',
#  'widefieldU.images.npy',
#  '_ibl_trials.table.pqt',
#  '_ibl_trials.goCueTrigger_times.npy',
#  '_ibl_trials.quiescencePeriod.npy',
#  '_ibl_trials.stimOff_times.npy',
#  '_ibl_trials.intervals_bpod.npy'
#  ]
#
# wfield_eids = pd.read_csv('2023_Q3_Findling_Hubert_et_al_wfield_sessions.csv', index_col=0)['session_id']
# wfield_datasets = Dataset.objects.filter(session__in=wfield_eids, name__in=wfield_dataset_types)
#
# pupil_eids = pd.read_csv('2023_Q3_Findling_Hubert_et_al_pupil_sessions.csv', index_col=0)['session_id']
# pupil_datasets = Dataset.objects.filter(session__in=pupil_eids, name='_ibl_leftCamera.lightningPose.pqt')
#
# datasets = wfield_datasets | pupil_datasets
#
# tag, _ = Tag.objects.get_or_create(name="2023_Q3_Findling_Hubert_et_al", protected=True, public=True)
# tag.datasets.set(datasets)
#
# # Save dataset IDs for release in public database
# dset_ids = [str(did) for did in datasets.values_list('pk', flat=True)]
# df = pd.DataFrame(dset_ids, columns=['dataset_id'])
# df.to_parquet('2023_Q3_Findling_Hubert_et_al_datasets.pqt')



"""""""""
# NEW CODE
# July 2025 renamed previous '2023_Q3_Findling_Hubert_et_al_datasets.pqt' created
# with old code above to '2023_Q3_Findling_Hubert_et_al_datasets_v1.pqt'
"""""""""

# Load in previous datasets and remove the bpod_intervals as we don't want to release these anymore otherwise we
# have problems with load_object for trials with mismatching dimensions
dset_orig = pd.read_parquet('2023_Q3_Findling_Hubert_et_al_datasets_v1.pqt')
orig_dsets = Dataset.objects.filter(id__in=dset_orig)
orig_dsets = orig_dsets.exlcude(name='_ibl_trials.intervals_bpod.npy')

wfield_eids = pd.read_csv('2023_Q3_Findling_Hubert_et_al_wfield_sessions.csv', index_col=0)['session_id']
sessions = Session.objects.filter(id__in=wfield_eids)

# Add new trials datasets (these are from a new revision after correcting for probabilityLeft values)
# Trials data
trials_dtypes = ['trials.goCueTrigger_times', 'trials.stimOff_times', 'trials.table', '_ibl_trials.quiescencePeriod']
trials_dsets = Dataset.objects.filter(session__in=wfield_eids, dataset_type__name__in=trials_dtypes, default_dataset=True)

# Passive data
passive_dtypes = ['_iblrig_RFMapStim.raw','_ibl_passivePeriods.intervalsTable', '_ibl_passiveRFM.times','_ibl_passiveGabor.table', '_ibl_passiveStims.table']
passive_dsets = Dataset.objects.filter(session__in=wfield_eids, dataset_type__name__in=passive_dtypes, default_dataset=True)

# Wheel data
wheel_dtypes = ['wheelMoves.intervals', 'wheelMoves.peakAmplitude', 'wheel.position', 'wheel.timestamps']
wheel_dsets = Dataset.objects.filter(session__in=wfield_eids, dataset_type__name__in=wheel_dtypes, default_dataset=True)

# Video data
vid_dtypes = ['camera.times', 'camera.dlc', 'camera.features', 'ROIMotionEnergy.position', 'camera.ROIMotionEnergy',
              'camera.lighteningPose']
video_dsets = Dataset.objects.none()
for sess in sessions:
    for cam in ['left', 'right', 'body']:
        if sess.extended_qc[f'video{cam.capitalize()}'] != 'CRITICAL':
            video_dsets = video_dsets | Dataset.objects.filter(session=sess,
                                                               name__icontains=cam,
                                                               dataset_type__name__in=vid_dtypes,
                                                               default_dataset=True)
            video_dsets = video_dsets | Dataset.objects.filter(session=sess, default_dataset=True,
                                                               name=f'_iblrig_{cam}Camera.raw.mp4')
# Licks times should be released if either left or right cam or both released
lick_sess = Session.objects.filter(id__in=video_dsets.values_list('session_id', flat=True))
for l in lick_sess:
    if not (l.extended_qc[f'videoLeft'] == 'CRITICAL' and l.extended_qc[f'videoRight'] == 'CRITICAL'):
        video_dsets = video_dsets | Dataset.objects.filter(session=l, default_dataset=True,
                                                           dataset_type__name=f'licks.times')

# Widefield data
wfield_dsets = Dataset.objects.filter(session__in=wfield_eids, collection='alf/widefield', default_dataset=True)

# Sync data
sync_dsets = Dataset.objects.filter(session__in=wfield_eids, collection='raw_ephys_data', default_dataset=True)



all_dsets = orig_dsets | trials_dsets | wheel_dsets | video_dsets | wfield_dsets | sync_dsets
all_dsets = all_dsets.distinct()

tag, _ = Tag.objects.get_or_create(name="2023_Q3_Findling_Hubert_et_al", protected=True, public=True)
tag.datasets.set(all_dsets)

# Save dataset IDs for release in public database
dset_ids = [str(did) for did in all_dsets.values_list('pk', flat=True)]
df = pd.DataFrame(dset_ids, columns=['dataset_id'])
df.to_parquet('2023_Q3_Findling_Hubert_et_al_datasets.pqt')



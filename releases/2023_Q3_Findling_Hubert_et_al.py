# %%
from pathlib import Path
import os
import pandas as pd
import alyx.base

from data.models import Tag, Dataset
from actions.models import Session
from django.db.models import Q

IBL_ALYX_ROOT = Path(alyx.base.__file__).parents[3].joinpath('iblalyx')
assert IBL_ALYX_ROOT.exists(), 'No IBL_ALYX_ROOT found, it is usually at the same directory level as the alyx repo'

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
# with old code above to '2023_Q3_Findling_Hubert_et_al_datasets_v1.pqt' and removed
the bpod_intervals datasets and resaved. These had to be removed to avoid problems with
load_object for trials with mismatching dimensions
"""""""""
DRY_RUN = True
script_dir = Path('/home/olivier/PycharmProjects/alyx_ferret/iblalyx/releases')
os.chdir(script_dir)
# Load in previous datasets
dsets_orig = pd.read_parquet('2023_Q3_Findling_Hubert_et_al_datasets_v1.pqt')
orig_dsets = Dataset.objects.filter(id__in=dsets_orig.dataset_id.values).distinct()

wfield_eids = pd.read_csv('2023_Q3_Findling_Hubert_et_al_wfield_sessions.csv', index_col=0)['session_id']
sessions = Session.objects.filter(id__in=wfield_eids)

# Add new trials datasets (these are from a new revision after correcting for probabilityLeft values)
# Trials data

trials_dtypes = ['trials.goCueTrigger_times', 'trials.stimOff_times', 'trials.table', '_ibl_trials.quiescencePeriod']
trials_dsets = Dataset.objects.filter(session__in=wfield_eids, dataset_type__name__in=trials_dtypes, default_dataset=True).distinct()

# Passive data
passive_dtypes = ['_iblrig_RFMapStim.raw', '_ibl_passivePeriods.intervalsTable', '_ibl_passiveRFM.times', '_ibl_passiveGabor.table',
                  '_ibl_passiveStims.table']
passive_dsets = Dataset.objects.filter(session__in=wfield_eids, dataset_type__name__in=passive_dtypes, default_dataset=True).distinct()

# Wheel data
wheel_dtypes = ['wheelMoves.intervals', 'wheelMoves.peakAmplitude', 'wheel.position', 'wheel.timestamps']
wheel_dsets = Dataset.objects.filter(session__in=wfield_eids, dataset_type__name__in=wheel_dtypes, default_dataset=True).distinct()


# Video data
def get_video_dsets(label):
    dnames = [
        f'_ibl_{label}Camera.lightningPose.pqt',
        f'_ibl_{label}Camera.dlc.pqt',
        f'_ibl_{label}Camera.times.npy',
        f'_ibl_{label}Camera.features.pqt',
        f'{label}Camera.ROIMotionEnergy.npy',
        f'{label}ROIMotionEnergy.position.npy',
        f'_iblrig_{label}Camera.raw.mp4'
    ]
    return dnames


# Get the video datasets
video_dsets = Dataset.objects.none()
for cam in ['left', 'right', 'body']:
    field_name = f"extended_qc__video{cam.capitalize()}"
    video_eids = sessions.exclude(**{field_name: 'CRITICAL'})
    dnames = get_video_dsets(cam)
    dsets = Dataset.objects.filter(session__in=video_eids, name__in=dnames, default_dataset=True).distinct()
    video_dsets = video_dsets | dsets

# Get the lick datasets
# If both left and right are critical we do not release the licks
lick_eids = sessions.exclude(Q(extended_qc__videoLeft='CRITICAL') & Q(extended_qc__videoRight='CRITICAL'))
dsets = (Dataset.objects.filter(session__in=lick_eids, name='licks.times.npy', default_dataset=True)
         .exclude(tags__name__icontains='brainwide')).distinct()
video_dsets = video_dsets | dsets
video_dsets = video_dsets.distinct()

# Widefield data
wfield_dsets = Dataset.objects.filter(session__in=wfield_eids, collection='alf/widefield', default_dataset=True).distinct()

# Sync data
sync_dsets = Dataset.objects.filter(session__in=wfield_eids, collection='raw_ephys_data', default_dataset=True).distinct()

# Get the full set of datasets
all_dsets = orig_dsets | trials_dsets | wheel_dsets | video_dsets | wfield_dsets | sync_dsets
all_dsets = all_dsets.distinct()

# Save dataset IDs for release in public database
dset_ids = [str(did) for did in all_dsets.values_list('pk', flat=True)]
df = pd.DataFrame(dset_ids, columns=['dataset_id'])
df.to_parquet(IBL_ALYX_ROOT.joinpath('2023_Q3_Findling_Hubert_et_al_datasets.pqt'))

if DRY_RUN is False:
    tag, _ = Tag.objects.get_or_create(name="2023_Q3_Findling_Hubert_et_al", protected=True, public=True)
    tag.datasets.set(all_dsets)

"""
- Spontaneous passive intervals for some sessions
- lightning pose : do not release lightning pose where the QC is critical: look at the extended QC json
- saturation npy files
- we will wait for the waveforms
- for the trials tables: do a read after write before
"""


from data.models import Dataset, Tag
from actions.models import Session
from django.db.models import Q
import pandas as pd

# Get the BWM sessions
dsets = Dataset.objects.filter(tags__name__icontains='brainwide', name__icontains='spikes.times')
bwm_sess = Session.objects.filter(id__in=dsets.values_list('session', flat=True).distinct())


"""
# Find the additional passive datasets to release
In 2023_Q4_IBL_et_al_BWM_passive we only released passive datasets that had all three components of the passive task
1. Spontaneous activity
2. Receptive field map
3. Passive task replay
In most instances it was the passive task replay that failed extraction. We have decided that it is valuable to release
the _ibl_passivePeriods.intervalsTable.csv and where available the _iblrig_RFMapStim.raw.bin datasets anyway for people
who want to do analysis on the spontaneous portion of the task. The sessions that have incomplete passive data have 
a note associated with them so they can be identified 
"""

# Note we do this in two steps because some have the '_iblrig_RFMapStim.raw.bin' but not '_ibl_passivePeriods.intervalsTable.csv'
# and we do not want to release these sessions.

# Find the sessions with the passivePeriods dataset that haven't already been released
dsets = Dataset.objects.filter(session__in=bwm_sess, name='_ibl_passivePeriods.intervalsTable.csv')
dsets = dsets.exclude(tags__name__icontains='brainwide')
add_pass_sess = dsets.values_list('session', flat=True).distinct()

# Get the relevant datasets
dnames = [
    '_ibl_passivePeriods.intervalsTable.csv',
    '_iblrig_RFMapStim.raw.bin'
]

dsets_passive = Dataset.objects.filter(session__in=add_pass_sess, name__in=dnames).distinct()


"""
# Find the additional lightening pose datasets to release
Lightening pose has been run on the full BWM dataset. The following datasets have been added / modified

Added:
_ibl_bodyCamera.lightningPose.pqt
_ibl_leftCamera.lightningPose.pqt
_ibl_rightCamera.lightningPose.pqt

Modified:
_ibl_bodyCamera.features.pqt
_ibl_leftCamera.features.pqt
_ibl_rightCamera.features.pqt
bodyCamera.ROIMotionEnergy.npy
leftCamera.ROIMotionEnergy.npy
rightCamera.ROIMotionEnergy.npy
bodyROIMotionEnergy.position.npy
leftROIMotionEnergy.position.npy
rightROIMotionEnergy.position.npy
licks.times.npy

We do not release the data from any of the BWM videos that have been set to CRITICAL
"""

def get_video_dsets(label):
    dnames = [
        f'_ibl_{label}Camera.lightningPose.pqt',
        f'_ibl_{label}Camera.features.pqt',
        f'{label}Camera.ROIMotionEnergy.npy',
        f'{label}ROIMotionEnergy.position.npy'
    ]

    return dnames

# Get the video datasets
dsets_video = Dataset.objects.none()
for cam in ['left', 'right', 'body']:
    field_name = f"extended_qc__video{cam.capitalize()}"
    video_eids = bwm_sess.exclude(**{field_name: 'CRITICAL'})
    dnames = get_video_dsets(cam)
    dsets = (Dataset.objects.filter(session__in=video_eids, name__in=dnames, default_dataset=True)
             .exclude(tags__name__icontains='brainwide')).distinct()
    dsets_video = dsets_video | dsets


# Get the lick datasets
# If both left and right are critical we do not release the licks
lick_eids = bwm_sess.exclude(Q(extended_qc__videoLeft='CRITICAL') & Q(extended_qc__videoRight='CRITICAL'))
dsets = (Dataset.objects.filter(session__in=lick_eids, name='licks.times.npy', default_dataset=True)
         .exclude(tags__name__icontains='brainwide')).distinct()

dsets_video = dsets_video | dsets


all_dsets = dsets_passive | dsets_video

# Tagging in production database
tag, _ = Tag.objects.get_or_create(name="2025_Q3_IBL_et_al_BWM", protected=True, public=True)
tag.datasets.set(all_dsets)

# Save dataset IDs for release in public database
dset_ids = [str(eid) for eid in dsets.values_list('pk', flat=True)]
df = pd.DataFrame(dset_ids, columns=['dataset_id'])
df.to_parquet('./2025_Q3_IBL_et_al_BWM_datasets.pqt')

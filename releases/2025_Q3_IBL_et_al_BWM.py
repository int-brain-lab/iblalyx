"""
- Spontaneous passive intervals for some sessions
- lightning pose : do not release lightning pose where the QC is critical: look at the extended QC json
- saturation npy files
- we will wait for the waveforms
- for the trials tables: do a read after write before
"""

# %%
from pathlib import Path
import pandas as pd

import alyx.base
from data.models import Dataset, Tag
from actions.models import Session
from django.db.models import Q

IBL_ALYX_ROOT = Path(alyx.base.__file__).parents[3].joinpath('iblalyx')
assert IBL_ALYX_ROOT.exists(), 'No IBL_ALYX_ROOT found, it is usually at the same directory level as the alyx repo'

def get_passive_datasets():
    """
    # Find the additional passive datasets to release
    In 2023_Q4_IBL_et_al_BWM_passive we only released passive datasets that had all three components of the passive task
    1. Spontaneous activity
    2. Receptive field map
    3. Passive task replay
    We have decided that it is valuable to release the _ibl_passivePeriods.intervalsTable.csv and where available
    the rfmap and task replay anyway for people who want to do analysis on the spontaneous portion of the task.
    The sessions that have incomplete passive data have a note associated with them so they can be identified
    """

    # Note we do this in two steps because some have the '_iblrig_RFMapStim.raw.bin' but not '_ibl_passivePeriods.intervalsTable.csv'
    # and we do not want to release these sessions.

    # Find the sessions with the passivePeriods dataset that haven't already been released
    dsets = Dataset.objects.filter(session__in=bwm_sess, name='_ibl_passivePeriods.intervalsTable.csv')
    dsets_intervals = dsets.exclude(tags__name__icontains='brainwide')
    add_pass_sess = dsets_intervals.values_list('session', flat=True).distinct()

    # Find the sessions that we have rfmap data times for and add this dataset as well as the RFMapStim file
    rfmap_dsets = Dataset.objects.filter(session__in=add_pass_sess, name='_ibl_passiveRFM.times.npy')
    rfmap_sess = rfmap_dsets.values_list('session', flat=True).distinct()

    bin_dsets = Dataset.objects.filter(session__in=rfmap_sess, name='_iblrig_RFMapStim.raw.bin', default_dataset=True)

    # Find the task replay datasets
    dnames = ['_ibl_passiveGabor.table.csv', '_ibl_passiveStims.table.csv']
    replay_dsets = Dataset.objects.filter(session__in=add_pass_sess, name__in=dnames, default_dataset=True)

    dsets_passive = dsets_intervals | rfmap_dsets | bin_dsets | replay_dsets
    # Save dataset IDs for release in public database
    dset_ids = [str(eid) for eid in dsets.values_list('pk', flat=True)]
    df = pd.DataFrame(dset_ids, columns=['dataset_id'])
    return df


def get_lightening_pose_datasets():
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
    dset_ids = [str(eid) for eid in dsets_video.values_list('pk', flat=True)]
    df = pd.DataFrame(dset_ids, columns=['dataset_id'])
    return df


def get_saturation_datasets():
    dsets = Dataset.objects.filter(name='_iblqc_ephysSaturationAP.samples.pqt')
    dset_ids = [str(eid) for eid in dsets.values_list('pk', flat=True)]
    df = pd.DataFrame(dset_ids, columns=['dataset_id'])
    return df

# %%
DRY_RUN = True
TAG_NAME = "2025_Q3_IBL_et_al_BWM"
dsets = Dataset.objects.filter(tags__name__icontains='brainwide', name__icontains='spikes.times')
bwm_sess = Session.objects.filter(id__in=dsets.values_list('session', flat=True).distinct())

df_datasets = []
df_datasets.append(get_passive_datasets())
df_datasets.append(get_lightening_pose_datasets())
df_datasets.append(get_saturation_datasets())

df_datasets.to_parquet(IBL_ALYX_ROOT.joinpath('releases', f'{TAG_NAME}.pqt'))

# %% Tagging in production database
if DRY_RUN is False:
    dsets2tag = Dataset.objects.filter(id__in=df_datasets['dataset_id'])
    tag, _ = Tag.objects.get_or_create(name=TAG_NAME, protected=True, public=True)
    tag.datasets.set(dsets2tag)


"""
Data Release request link:
https://docs.google.com/document/d/1ziHSzUoGWHMi8YU3JtCr2Q8QTlbZUllmtxxarEY9ab4/edit?tab=t.0
"""
from data.models import Dataset, Tag

TAG_NAME = '2025_Q3_Zang_et_al_Aging'



# Video datasets
sess = Session.objects.filter(id__in=eids)

# Make sure only non-critical video datasets are released
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
dsets_video = Dataset.objects.none()
for cam in ['left', 'right', 'body']:
    field_name = f"extended_qc__video{cam.capitalize()}"
    video_eids = sess.exclude(**{field_name: 'CRITICAL'})
    dnames = get_video_dsets(cam)
    dsets = Dataset.objects.filter(session__in=video_eids, name__in=dnames, default_dataset=True).distinct()
    dsets_video = dsets_video | dsets


# Get the lick datasets
# If both left and right are critical we do not release the licks
lick_eids = sess.exclude(Q(extended_qc__videoLeft='CRITICAL') & Q(extended_qc__videoRight='CRITICAL'))
dsets = (Dataset.objects.filter(session__in=lick_eids, name='licks.times.npy', default_dataset=True)
         .exclude(tags__name__icontains='brainwide')).distinct()

dsets_video = dsets_video | dsets

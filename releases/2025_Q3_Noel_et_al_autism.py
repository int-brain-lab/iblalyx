"""

Other data repository: https://osf.io/fap2s/
Nature Neuroscience | Volume 28 | July 2025 | 1519–1532 1519 nature neuroscience
https://doi.org/10.1038/s41593-025-01965-8

https://osf.io/fap2s/wiki/home/

- for the trials tables: do a read after write before

"""

# %%
from pathlib import Path
import pandas as pd  # uv pip install openpyxl
from data.models import Dataset
from actions.models import Session
from django.db.models import Q

TAG_NAME = '2025_Q3_Noel_et_al_Autism'

project_name = 'angelaki_mouseASD'

for xls_file in Path('/home/olivier/scratch/autism').glob('*.xlsx'):
    df = pd.read_excel(xls_file)
    break

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

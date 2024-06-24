from actions.models import Session
from data.models import Dataset, Tag
import pandas as pd

eids = [
    '46794e05-3f6a-4d35-afb3-9165091a5a74',  # churchlandlab/CSHL045/2020-02-27/001
    'c7b0e1a3-4d4d-4a76-9339-e73d0ed5425b',  # cortexlab/KS020/2020-02-06/001
    'db4df448-e449-4a6f-a0e7-288711e7a75a',  # danlab/DY_009/2020-02-27/001
    '54238fd6-d2d0-4408-b1a9-d19d24fd29ce',  # danlab/DY_018/2020-10-15/001
    'f3ce3197-d534-4618-bf81-b687555d1883',  # hoferlab/SWC_043/2020-09-15/001
    '493170a6-fd94-4ee4-884f-cc018c17eeb9',  # hoferlab/SWC_061/2020-11-23/001
    '7cb81727-2097-4b52-b480-c89867b5b34c',  # mrsicflogellab/SWC_052/2020-10-22/001
    '1735d2be-b388-411a-896a-60b01eaa1cfe',  # mrsicflogellab/SWC_058/2020-12-11/001
    'ff96bfe1-d925-4553-94b5-bf8297adf259',  # wittenlab/ibl_witten_26/2021-01-27/002
    '73918ae1-e4fd-4c18-b132-00cb555b1ad2',  # wittenlab/ibl_witten_27/2021-01-21/001
]

datasets = [
    'wheel.position',
    'wheel.timestamps',
    'leftCamera.times',
    'leftCamera.raw',
    'leftCamera.dlc',
]
wheel_dtypes = ['wheel.position', 'wheel.timestamps']
wheel_dsets = Dataset.objects.filter(session__in=eids, dataset_type__name__in=wheel_dtypes, default_dataset=True)
video_dtypes = ['camera.times', 'camera.dlc', '_iblrig_Camera.raw']
video_dsets = Dataset.objects.filter(session__in=eids, name__icontains='left', dataset_type__name__in=video_dtypes,
                                     default_dataset=True)
dsets = wheel_dsets | video_dsets
dsets = dsets.distinct()

# Check we have the correct number of datasets
assert dsets.count() == len(eids) * len(datasets)

# Check that for each dataset we have one per session
for d in datasets:
    assert len(set(dsets.filter(name__icontains=d).values_list('session'))) == len(eids)

# Save the dataset ids
dids = [str(d.id) for d in dsets]
df = pd.DataFrame(dids, columns=['dataset_id'])
df.to_parquet('./2024_Q2_Blau_et_al_datasets.pqt')

# Tag the datasets
tag, _ = Tag.objects.get_or_create(name="2024_Q2_Blau_et_al", protected=True, public=True)
tag.datasets.set(dsets)

# Save the sessions and the insertions
df = pd.DataFrame(
    columns=['eid'],
    data=eids)
df.to_csv('./2024_Q2_Blau_et_al_sessions.csv')



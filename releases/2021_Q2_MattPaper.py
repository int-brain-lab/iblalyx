import pandas as pd
from data.models import Tag, Dataset

# Releases as part of paper Matt Whiteway et al, 2021, DOI: 110.1371/journal.pcbi.1009439

# Original query
eid_1 = '8181ca89-42b7-4ff2-a0f9-4e609d6f5c67'
dtype_1 = '_iblrig_Camera.raw'

eid_2 = '89f0d6ff-69f4-45bc-b89e-72868abb042a'
dnames_2 = [
    '_ibl_leftCamera.dlc.pqt',
    '_ibl_leftCamera.times.npy',
    '_iblrig_leftCamera.raw.mp4'
]
dsets = (Dataset.objects.filter(session=eid_1, dataset_type__name=dtype_1) |
         Dataset.objects.filter(session=eid_2, name__in=dnames_2))

# Tagging in production database
tag, _ = Tag.objects.get_or_create(name="Matt's paper", protected=True, public=True)
for dset in dsets:
    dset.tags.add(tag)

# Save dataset IDs for release in public database
dset_ids = [str(eid) for eid in dsets.values_list('pk', flat=True)]
df = pd.DataFrame(dset_ids, columns=['dataset_id'])
df.to_csv('./2021_Q2_MattPaper.csv')

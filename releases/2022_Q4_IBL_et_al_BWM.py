import pandas as pd
from data.models import Tag, Dataset

df = pd.read_csv('2022_Q4_IBL_et_al_BWM_eids_pids.csv', index_col=0)

# Anything with wheel
wheel = Dataset.objects.filter(session_id__in=df['eid'], name__icontains='wheel')
# Anything with trials
trials = Dataset.objects.filter(session_id__in=df['eid'], name__icontains='trials')
# DLC and motion energy
ds_types = ['camera.times', 'camera.dlc', 'camera.ROIMotionEnergy', 'ROIMotionEnergy.position', 'camera.times']
cam = Dataset.objects.filter(session_id__in=df['eid'], dataset_type__name__in=ds_types)

# for all pids protect pykilosort data
pyks = Dataset.objects.filter(probe_insertion__id__in=df['pid'], collection__icontains='pykilosort')

# Combine all and tag
dsets = wheel | trials | cam | pyks
tag, _ = Tag.objects.get_or_create(name="2022_Q4_IBL_et_al_BWM", protected=True, public=True)
tag.datasets.set(dsets)

# Save dataset IDs
dset_ids = [d.id for d in dsets]
df = pd.DataFrame(dset_ids, columns=['dataset_id'])
df.to_parquet('./2022_Q4_IBL_et_al_BWM_datasets.pqt')

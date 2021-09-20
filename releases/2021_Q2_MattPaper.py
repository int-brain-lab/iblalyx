from data.models import Tag, Dataset, DatasetType
from experiments.models import ProbeInsertion

## Matt video paper
eid = '8181ca89-42b7-4ff2-a0f9-4e609d6f5c67'  # [<Session: 8181ca89-42b7-4ff2-a0f9-4e609d6f5c67 IBL-T4/2019-04-23/001>]
dsets = Dataset.objects.filter(session=eid, dataset_type__name='_iblrig_Camera.raw')
tag, _ = Tag.objects.get_or_create(name="Matt's paper", protected=True, public=True)
for dset in dsets:
    dset.tags.add(tag)

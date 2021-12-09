from data.models import Tag, Dataset, DatasetType
from experiments.models import ProbeInsertion

## Matt video paper
eid = '8181ca89-42b7-4ff2-a0f9-4e609d6f5c67'  # [<Session: 8181ca89-42b7-4ff2-a0f9-4e609d6f5c67 IBL-T4/2019-04-23/001>]
dsets = Dataset.objects.filter(session=eid, dataset_type__name='_iblrig_Camera.raw')
tag, _ = Tag.objects.get_or_create(name="Matt's paper", protected=True, public=True)
for dset in dsets:
    dset.tags.add(tag)

eid2 = '89f0d6ff-69f4-45bc-b89e-72868abb042a'  # churchlandlab/CSHL047/2020-01-20/001
# one.list_datasets(eid2, dict(object='leftCamera'))
datasets = [
    '_ibl_leftCamera.dlc.pqt',
    '_ibl_leftCamera.times.npy',
    '_iblrig_leftCamera.raw.mp4'
]
dsets = Dataset.objects.filter(session=eid2, name__in=datasets)
for dset in dsets:
    dset.tags.add(tag)

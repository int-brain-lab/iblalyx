import pandas as pd

from data.models import Tag, Dataset, DatasetType
from experiments.models import ProbeInsertion

# Pre-release for Annual meeting may 2021

# Original query
pids = ["da8dfec1-d265-44e8-84ce-6ae9c109b8bd",  # SWC_043_2020-09-21_probe00 ok
        "b749446c-18e3-4987-820a-50649ab0f826",  # KS023_2019-12-10_probe01  ok
        "f86e9571-63ff-4116-9c40-aa44d57d2da9",  # CSHL049_2020-01-08_probe00 a bit stripy but fine
        "675952a4-e8b3-4e82-a179-cc970d5a8b01"]  # CSH_ZAD_029_2020-09-19_probe01 a bit stripy as well

dtypes = list(pd.read_csv('2021_Q2_PreRelease_dtypes.csv')['dataset_type'])

probe_insertions = ProbeInsertion.objects.filter(id__in=pids)
datasets = Dataset.objects.filter(
    session__in=probe_insertions.values_list('session', flat=True),
    dataset_type__name__in=dtypes)
# remove the datasets from other probes
for ins in probe_insertions:
    other_probes = ProbeInsertion.objects.filter(session=ins.session).exclude(pk=ins.pk)
    datasets = datasets.exclude(probe_insertion__in=other_probes)

# Tagging in production database
tag, _ = Tag.objects.get_or_create(name="2021_Q2_PreRelease", protected=True, public=True)
for dset in datasets:
    dset.tags.add(tag)

# Save dataset IDs for release in public database
dset_ids = [str(eid) for eid in datasets.values_list('pk', flat=True)]
df = pd.DataFrame(dset_ids, columns=['dataset_id'])
df.to_parquet('./2021_Q2_PreRelease_datasets.pqt')

from data.models import Tag, Dataset
from experiments.models import ProbeInsertion

## Pre-release Annual meeting may 2021
pids = ["da8dfec1-d265-44e8-84ce-6ae9c109b8bd",  # SWC_043_2020-09-21_probe00 ok
        "b749446c-18e3-4987-820a-50649ab0f826",  # KS023_2019-12-10_probe01  ok
        "f86e9571-63ff-4116-9c40-aa44d57d2da9",  # CSHL049_2020-01-08_probe00 a bit stripy but fine
        "675952a4-e8b3-4e82-a179-cc970d5a8b01"]  # CSH_ZAD_029_2020-09-19_probe01 a bit stripy as well]

tag, _ = Tag.objects.get_or_create(name="May 2021 pre-release", protected=True, public=True)

probe_insertions = ProbeInsertion.objects.filter(id__in=pids)
datasets = Dataset.objects.filter(session__in=probe_insertions.values_list('session', flat=True))
# remove the datasets from other probes
print(datasets.count())
for ins in probe_insertions:
    other_probes = ProbeInsertion.objects.filter(session=ins.session).exclude(pk=ins.pk)
    datasets = datasets.exclude(probe_insertion__in=other_probes)
print(datasets.count())

tag.datasets.set(datasets)

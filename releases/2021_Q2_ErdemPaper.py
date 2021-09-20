from data.models import Tag, Dataset, DatasetType
from experiments.models import ProbeInsertion

## Erdem registration stunt
# CSHL047_2020-01-20_001_alf_probe00_a_raw.png
eid = '89f0d6ff-69f4-45bc-b89e-72868abb042a'   # CSHL047_2020-01-20_001_alf_probe00_a_raw.png
pid = '341ef9bb-25f9-4eeb-8f1d-bdd054b22ba8'
dsets = Dataset.objects.filter(probe_insertion=pid, name__istartswith='_spikeglx_ephysData_g0_t0.imec.ap')

tag, _ = Tag.objects.get_or_create(name="Erdem's paper", protected=True, public=True)
tag.datasets.set(dsets)

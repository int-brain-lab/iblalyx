import pandas as pd
from data.models import Tag, Dataset

# Releases as part of paper Erdem Varol et al, 2021, DOI: 10.1109/ICASSP39728.2021.9414145

# Original query
eid = '89f0d6ff-69f4-45bc-b89e-72868abb042a'
pid = '341ef9bb-25f9-4eeb-8f1d-bdd054b22ba8'
dsets = Dataset.objects.filter(probe_insertion=pid, name__istartswith='_spikeglx_ephysData_g0_t0.imec.ap')

# For retrospective documenting datatypes only, don't use this for querying as you'll get some LF files as well
dtypes = [
    'ephysData.raw.ap',
    'ephysData.raw.ch',
    'ephysData.raw.meta'
    ]
assert all([dtype in dtypes for dtype in dsets.values_list('dataset_type__name', flat=True)])

# Tagging of datasets in the production database
tag, _ = Tag.objects.get_or_create(name="2021_Q2_Varol_et_al", protected=True, public=True)
tag.datasets.set(dsets)

# Saving dataset IDs for release in the public database
dset_ids = [str(eid) for eid in dsets.values_list('pk', flat=True)]
df = pd.DataFrame(dset_ids, columns=['dataset_id'])
df.to_parquet('./2021_Q2_Varol_et_al_datasets.pqt')

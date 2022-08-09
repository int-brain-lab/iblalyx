import pandas as pd
from data.models import Tag, Dataset

# Releases as part of paper The International Brain Laboratory et al, 2022, DOI:

# Original query based on list of session ids
eids = list(pd.read_parquet('2022_Q3_IBL_et_al_DAWG_sessions.pqt')['session_id'])
datasets = Dataset.objects.filter(session__pk__in=eids, dataset_type__name='_iblrig_ambientSensorData.raw').distinct()
print(len(eids))
print(datasets.count())

tag, _ = Tag.objects.get_or_create(name="2022_Q3_IBL_et_al_DAWG", protected=True, public=True)
tag.datasets.set(datasets)

df = pd.DataFrame(columns=['dataset_id'])
df['dataset_id'] = [str(x) for x in datasets.values_list('pk', flat=True)]
df.to_parquet('2022_Q3_IBL_et_al_DAWG_datasets.pqt')

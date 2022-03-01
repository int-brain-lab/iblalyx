import pandas as pd
from data.models import Tag, Dataset, DatasetType

# Releases as part of paper The International Brain Laboratory et al, 2021, DOI: 10.7554/eLife.63711

# Original query based on list of session ids
eids = list(pd.read_csv('2021_Q1_BehaviourPaper_sessions.csv', index_col=0)['session_id'])
dtypes = [
            'trials.feedback_times',
            'trials.feedbackType',
            'trials.intervals',
            'trials.choice',
            'trials.response_times',
            'trials.contrastLeft',
            'trials.contrastRight',
            'trials.probabilityLeft',
            'trials.stimOn_times',
            'trials.repNum',
            'trials.goCue_times',
            ]
dataset_types = DatasetType.objects.filter(name__in=dtypes)
dsets = Dataset.objects.filter(session__in=eids, dataset_type__in=dataset_types)

# Tagging in production database
tag, _ = Tag.objects.get_or_create(name="Behaviour Paper", protected=True, public=True)
for dset in dsets:
    dset.tags.add(tag)

# Saving dataset IDs for release in the public database
dset_ids = [str(eid) for eid in dsets.values_list('pk', flat=True)]
df = pd.DataFrame(dset_ids, columns=['dataset_id'])
df.to_parquet('./2021_Q1_BehaviourPaper_datasets.pqt')

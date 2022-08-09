import pandas as pd
from django.db.models import Q
from data.models import Tag, Dataset, DatasetType
from actions.models import Session

# Releases as part of paper The International Brain Laboratory et al, 2021, DOI: 10.7554/eLife.63711

# Original query based on list of session ids
eids = list(pd.read_csv('2021_Q1_IBL_et_al_Behaviour_sessions.csv', index_col=0)['session_id'])

# First get the trials.table datasets for all session that they are available for
tables_ds = Dataset.objects.filter(session__in=eids, dataset_type__name='trials.table')
# For the rest of the sessions, get the individual datasets
sess_no_table = Session.objects.filter(pk__in=eids).filter(~Q(pk__in=tables_ds.values_list('session_id')))
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
            'trials.goCue_times',
            ]
dataset_types = DatasetType.objects.filter(name__in=dtypes)
indv_ds = Dataset.objects.filter(session__in=sess_no_table, dataset_type__in=dataset_types)

# For all sessions, get the reNum data (not included in trials.table)
repnum_ds = Dataset.objects.filter(session__in=eids, dataset_type__name='trials.repNum')

# Bring all datasets together
dsets = tables_ds | indv_ds | repnum_ds

# Tagging in production database
tag, _ = Tag.objects.get_or_create(name="2021_Q1_IBL_et_al_Behaviour", protected=True, public=True)
tag.datasets.set(dsets)

# Saving dataset IDs for release in the public database
dset_ids = [str(eid) for eid in dsets.values_list('pk', flat=True)]
df = pd.DataFrame(dset_ids, columns=['dataset_id'])
df.to_parquet('./2021_Q1_IBL_et_al_Behaviour_datasets.pqt')

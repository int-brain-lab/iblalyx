import pandas as pd
from data.models import Tag, Dataset, FileRecord
from subjects.models import Subject

"""""""""""""""
Original code
"""""""""""""""
# subjects = Tag.objects.get(name__icontains='Behaviour').datasets.values_list('session__subject', flat=True).distinct()
# subjects = Subject.objects.filter(pk__in=subjects)
#
# rel_paths = [f'Subjects/{s.lab.name}/{s.nickname}/_ibl_subjectTrials.table.pqt' for s in subjects]
# fr = FileRecord.objects.filter(relative_path__in=rel_paths)
# datasets = Dataset.objects.filter(file_records__in=fr).distinct()
#
# tag, _ = Tag.objects.get_or_create(name="2023_Q1_Mohammadi_et_al", protected=True, public=True)
# tag.datasets.set(datasets)
#
# # Save dataset IDs for release in public database
# dset_ids = [str(eid) for eid in datasets.values_list('pk', flat=True)]
# df = pd.DataFrame(dset_ids, columns=['dataset_id'])
# df.to_parquet('./2023_Q1_Mohammadi_et_al_datasets.pqt')


"""""""""""""""""""""""""""""""""""""""""""""""""""""""""
Adapted code to include subjectSessions.table Feb 2025
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""

# Load in the original datasets (note 2023_Q1_Mohammadi_et_al_datasets.pqt was renamed to
# 2023_Q1_Mohammadi_et_al_datasets_v1.pqt in Feb 2025)
orig_dsets = pd.read_parquet('./2023_Q1_Mohammadi_et_al_datasets_v1.pqt')
dsets = Dataset.objects.filter(id__in=orig_dsets['dataset_id'].values)

subjects = Subject.objects.filter(id__in=dsets.values_list('object_id', flat=True))
session_dsets = Dataset.objects.filter(name='_ibl_subjectSessions.table.pqt', object_id__in=subjects.values_list('id', flat=True))
datasets = dsets | session_dsets

tag, _ = Tag.objects.get_or_create(name="2023_Q1_Mohammadi_et_al", protected=True, public=True)
tag.datasets.set(datasets)

# Save dataset IDs for release in public database
dset_ids = [str(eid) for eid in datasets.values_list('pk', flat=True)]
df = pd.DataFrame(dset_ids, columns=['dataset_id'])
df.to_parquet('./2023_Q1_Mohammadi_et_al_datasets.pqt')



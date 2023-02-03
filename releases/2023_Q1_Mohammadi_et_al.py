import pandas as pd
from data.models import Tag, Dataset, FileRecord
from subjects.models import Subject

subjects = Tag.objects.get(name__icontains='Behaviour').datasets.values_list('session__subject', flat=True).distinct()
subjects = Subject.objects.filter(pk__in=subjects)

rel_paths = [f'Subjects/{s.lab.name}/{s.nickname}/_ibl_subjectTrials.table.pqt' for s in subjects]
fr = FileRecord.objects.filter(relative_path__in=rel_paths)
datasets = Dataset.objects.filter(file_records__in=fr).distinct()

tag, _ = Tag.objects.get_or_create(name="2023_Q1_Mohammadi_et_al", protected=True, public=True)
tag.datasets.set(datasets)

# Save dataset IDs for release in public database
dset_ids = [str(eid) for eid in datasets.values_list('pk', flat=True)]
df = pd.DataFrame(dset_ids, columns=['dataset_id'])
df.to_parquet('./2023_Q1_Mohammadi_et_al_datasets.pqt')
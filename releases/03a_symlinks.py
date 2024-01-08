'''
After pruning the public database to the released datasets, this script creates symlinks in publicly available folder.
This script needs to be run on the SDSC server with alyxvenv activated
'''

from pathlib import Path
from data.models import Dataset, FileRecord

datasets = Dataset.objects.using('public').all()
ndsets = datasets.count()
# Check that all datasets have an FI file record, otherwise flag
file_records = FileRecord.objects.using('public').filter(dataset__in=datasets, data_repository__name__startswith='flatiron')
if file_records.count() == ndsets:
    pass
else:
    diffs = datasets.values_list('id', flat=True).difference(file_records.values_list('dataset_id', flat=True))
    for diff in diffs:
        print(f"...no file record for dataset with ID: {str(diff)}")

# This part remains a loop, didn't find a better solution
c = 0
for fr in file_records:
    rel_path = fr.data_url.split('public')[1].strip('/')
    source = Path('/mnt/ibl').joinpath(rel_path)
    dest = Path('/mnt/ibl/public').joinpath(rel_path)
    if source.exists():
        if dest.exists():
            pass
        else:
            dest.parent.mkdir(exist_ok=True, parents=True)
            dest.symlink_to(source)
    else:
        print(f'...source does not exist: {source}')
    c += 1
    if c % 20000 == 0:
        print(f"creating symlinks {c}/{ndsets}")

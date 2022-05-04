'''
After pruning the public database to the released datasets, this script creates symlinks in publicly available folder.
This script needs to be run on the SDSC server with alyxvenv activated
'''

from pathlib import Path
from data.transfers import _add_uuid_to_filename
from data.models import DataRepository, Dataset

datasets = Dataset.objects.using('public').all()
ndsets = datasets.count()
print(f"Starting to create {ndsets} symlinks")
c = 0
for dset in datasets:
    fr = dset.file_records.filter(data_repository__name__startswith='flatiron').first()
    if fr is None:
        print(f"...no file record for dataset with ID: {str(dset.pk)}")
    else:
        rel_path = Path(fr.data_repository.globus_path).joinpath(fr.relative_path).relative_to('/')
        rel_path = _add_uuid_to_filename(str(rel_path), dset.pk)
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

print("Finished creating symlinks\n")

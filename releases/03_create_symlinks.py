"""
This script creates symlinks in publicly available folder.
This script needs to be run on the SDSC server with alyxvenv activated

Usage:
    python 03_create_symlinks.py [--tags TAG1 TAG2 ...]

Arguments:
    --tags          Optional list of Tag names to process. If provided, only datasets with these tags
                    will be processed. If not provided, all datasets in the public database will be processed.

Examples:
    # Process all datasets in the public database
    python 03_create_symlinks.py

    # Process only datasets with specific tags
    python 03_create_symlinks.py --tags IBL-learning IBL-behavior

Notes:
    - The script checks if all datasets have a file record on Flatiron
    - It creates symlinks from /mnt/ibl/[path] to /mnt/ibl/public/[path]
    - Missing source files are reported but don't stop the process
    - Existing symlinks are skipped
"""

from pathlib import Path
import argparse
import tqdm

from data.models import Dataset, FileRecord


def create_symlinks(tag_names=None):
    if tag_names is None:
        datasets = Dataset.objects.using('public').all()
    else:
        datasets = Dataset.objects.using('public').filter(tag__name__in=tag_names)
    ndsets = datasets.count()
    # Check that all datasets have an FI file record, otherwise flag
    file_records = FileRecord.objects.using('public').filter(data_repository__name__startswith='flatiron').order_by('-dataset__auto_datetime')
    if file_records.count() == ndsets:
        pass
    else:
        diffs = datasets.values_list('id', flat=True).difference(file_records.values_list('dataset_id', flat=True))
        for diff in diffs:
            print(f"...no file record for dataset with ID: {str(diff)}")

    # This part remains a loop, didn't find a better solution
    for fr in tqdm.tqdm(file_records):
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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Create symlinks for publicly released datasets on SDSC server'
    )
    parser.add_argument('--tags', nargs='+', type=str,
                        help='Optional list of Tag names to process.')

    args = parser.parse_args()
    create_symlinks(args.tags)

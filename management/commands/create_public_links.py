"""
Django management command to create symlinks in publicly available folder.

This command needs to be run on the SDSC server with alyxvenv activated.

Usage:
    python manage.py create_public_links [--tags TAG1 TAG2 ...]

Arguments:
    --tags          Optional list of Tag names to process. If provided, only datasets with these tags
                    will be processed. If not provided, all datasets in the public database will be processed.

Examples:
    # Process all datasets in the public database
    python manage.py openalyx

    # Process only datasets with specific tags
    python manage.py openalyx --tags IBL-learning IBL-behavior

Notes:
    - The command checks if all datasets have a file record on Flatiron
    - It creates symlinks from /mnt/ibl/[path] to /mnt/ibl/public/[path]
    - Missing source files are reported but don't stop the process
    - Existing symlinks are skipped
"""

from pathlib import Path
import tqdm

from django.core.management.base import BaseCommand

from data.models import Dataset, FileRecord


class Command(BaseCommand):
    help = 'Create symlinks for publicly released datasets on SDSC server'

    def add_arguments(self, parser):
        parser.add_argument('--tags', nargs='+', type=str,
                            help='Optional list of Tag names to process.')

    def handle(self, *args, **options):
        tag_names = options.get('tags')

        if tag_names is None:
            self.stdout.write(self.style.SUCCESS('Processing all datasets in public database'))
            datasets = Dataset.objects.using('public').all()
        else:
            self.stdout.write(self.style.SUCCESS(f'Filtering datasets by tags: {", ".join(tag_names)}'))
            datasets = Dataset.objects.using('public').filter(tags__name__in=tag_names).distinct()
            self.stdout.write(self.style.SUCCESS(f'Found {datasets.count()} datasets with specified tags'))

        ndsets = datasets.count()

        # Check that all datasets have an FI file record, otherwise flag
        file_records = FileRecord.objects.using('public').filter(
            data_repository__name__startswith='flatiron',
            dataset__in=datasets
        ).order_by('-dataset__auto_datetime')

        if file_records.count() == ndsets:
            self.stdout.write(self.style.SUCCESS(f'All {ndsets} datasets have file records on Flatiron.'))
        else:
            diffs = datasets.values_list('id', flat=True).difference(file_records.values_list('dataset_id', flat=True))
            self.stdout.write(self.style.WARNING(f'Warning: {len(diffs)} datasets are missing file records on Flatiron:'))
            for diff in diffs:
                self.stdout.write(f'...no file record for dataset with ID: {str(diff)}')

        # Create symlinks for all file records
        self.stdout.write(self.style.SUCCESS(f'Creating symlinks for {file_records.count()} file records...'))
        created = 0
        skipped = 0
        missing = 0

        for fr in tqdm.tqdm(file_records):
            rel_path = fr.data_url.split('public')[1].strip('/')
            source = Path('/mnt/ibl').joinpath(rel_path)
            dest = Path('/mnt/ibl/public').joinpath(rel_path)

            if source.exists():
                if dest.exists():
                    skipped += 1
                else:
                    dest.parent.mkdir(exist_ok=True, parents=True)
                    dest.symlink_to(source)
                    created += 1
            else:
                self.stdout.write(f'...source does not exist: {source}')
                missing += 1

        self.stdout.write(self.style.SUCCESS(
            f'Summary: {created} symlinks created, {skipped} already existed, {missing} source files missing'
        ))

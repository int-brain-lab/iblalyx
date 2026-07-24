"""
Django management command to (re)assign public release tags to their expected datasets.

Each public release has a tag (by ID) and a parquet file of dataset IDs (in iblalyx/releases/)
that should carry that tag. This checks that iblalyx.releases.utils.PUBLIC_DS_TAGS/PUBLIC_DS_FILES
are in sync with the tags on the database, and by default raises if any tag's datasets don't match.

This must run against the main production database (i.e. via ibl_alyx_apache), before a release,
not against a buffer database.

Usage:
    python manage.py update_tags [--update]

Arguments:
    --update        Instead of raising on a mismatch, update the tag's datasets to match the
                    parquet file.

Examples:
    # Check all release tags match their parquet files, raise if any don't
    python manage.py update_tags

    # Fix any tags whose datasets don't match their parquet file
    python manage.py update_tags --update
"""

import sys
from pathlib import Path

import pandas as pd

from django.core.management.base import BaseCommand, CommandError

from data.models import Dataset, Tag

# iblalyx is bind-mounted whole into the container at /home/iblalyx (see docker-compose.yaml),
# separately from this command file, so it isn't reachable via a relative path from __file__.
IBLALYX_ROOT = Path('/home/iblalyx')
sys.path.insert(0, str(IBLALYX_ROOT.parent))
from iblalyx.releases.utils import PUBLIC_DS_FILES, PUBLIC_DS_TAGS


class Command(BaseCommand):
    help = "Check (or update) that public release tags' datasets match their parquet files"

    def add_arguments(self, parser):
        parser.add_argument('--update', action='store_true',
                             help="Update mismatched tags' datasets instead of raising an error.")

    def handle(self, *args, **options):
        update = options['update']
        mismatches = []

        for pdn, tagid in zip(reversed(PUBLIC_DS_FILES), reversed(PUBLIC_DS_TAGS)):
            pdf = IBLALYX_ROOT.joinpath('releases', pdn)
            tag = Tag.objects.get(id=tagid)
            datasets = Dataset.objects.filter(pk__in=list(pd.read_parquet(pdf)['dataset_id']))

            if set(tag.datasets.all()) == set(datasets):
                self.stdout.write(f'{pdn}: all tags matching, skip')
            elif update:
                self.stdout.write(self.style.WARNING(f'{pdn}: updating tags for {datasets.count()} datasets'))
                tag.datasets.set(datasets)
            else:
                mismatches.append(pdn)
                self.stdout.write(self.style.ERROR(f'{pdn}: tags do not match'))

        if mismatches and not update:
            raise CommandError(
                f"{len(mismatches)} tag(s) don't match their expected datasets: {', '.join(mismatches)}. "
                "Re-run with --update to fix."
            )

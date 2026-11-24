"""These are the sessions we're currently using for analysis.

They are not yet released but we maintain a tag for easy of querying"""
import sys
import os
from pathlib import Path

import pandas as pd
import django

if __name__ == '__main__' and not os.environ.get('DJANGO_SETTINGS_MODULE'):
    sys.path.insert(0, '.')
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'alyx.settings')
    django.setup()


from django.db.models import Q
from actions.models import Session
from data.models import Dataset, Tag
from experiments.models import ProbeInsertion

import alyx.base

RELEASES_DIR = Path(__file__).resolve().parent
IBL_ALYX_ROOT = RELEASES_DIR.parent
# if not IBL_ALYX_ROOT.exists():
#     IBL_ALYX_ROOT = Path('/home/iblalyx')
assert IBL_ALYX_ROOT.exists(), 'No IBL_ALYX_ROOT found, it is usually at the same directory level as the alyx repo'

DRY_RUN = True
TAG_NAME = '2026_Q4_mesoscope_canonical'
# CSV of subject/date/number
URL = 'https://raw.githubusercontent.com/int-brain-lab/mesoscope/refs/heads/main/canonical_sessions.txt'
sessions = pd.read_csv(URL, header=None, sep=r',\s+', engine='python', ).squeeze().values.tolist()

eids = []
for subject, date, number in map(lambda x: x.split('/'), sessions):
    try:
        sess = Session.objects.get(subject__nickname=subject, start_time__date=date, number=int(number), type='Experiment')
        eids.append(sess.id)
    except Session.DoesNotExist:
        print(f'No session found for {subject} on {date} with number {number}')
        continue

# For each eid, get all datasets and add them to a tag
dsets = Dataset.objects.filter(session__id__in=eids).values_list('id', flat=True)

if DRY_RUN is False:
    tag, _ = Tag.objects.get_or_create(
        name=TAG_NAME, protected=False, public=False,
        description='Canonical sessions in the mesoscope data analysis. These sessions are not yet released but we maintain a tag for easy of querying'
    )
    tag.datasets.set(dsets)

# python manage.py one_cache -v 2 --compress --qc --tag $TAG_NAME -D s3://ibl-brain-wide-map-public/caches/alyx/$TAG_NAME
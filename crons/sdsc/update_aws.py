"""Script to query recently-updated datasets and sync those specific sessions with AWS"""
import time
from pathlib import Path
import datetime
import subprocess
import logging

from one.alf.files import folder_parts, get_session_path, get_alf_path
import pandas as pd
from django.core.paginator import Paginator

from data.models import DataRepository, Dataset, FileRecord

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
batch_size = 50000
nuo = datetime.datetime.now() - datetime.timedelta(hours=24)  # ~3000 datasets
r = DataRepository.objects.filter(name__startswith='aws').first()
assert r
bucket_name = r.json['bucket_name']
if not bucket_name.startswith('s3:'):
    bucket_name = 's3://' + bucket_name
qs = Dataset.objects.filter(auto_datetime__gt=nuo).order_by('created_datetime')
# Ugly hack because globus_path doesn't actually contain the correct absolute path
root = '/mnt/ibl'  # This should be in the globus_path but isn't

paginator = Paginator(qs, batch_size)

# fields to keep from Dataset table
dataset_fields = ('id', 'session', 'auto_datetime')
filerecord_fields = ('dataset_id', 'relative_path', 'data_repository__globus_path')

all_df = []
for i in paginator.page_range:
    data = paginator.get_page(i)
    current_qs = data.object_list
    df = pd.DataFrame.from_records(current_qs.values(*dataset_fields))
    frs = FileRecord.objects.select_related('data_repository').filter(dataset_id__in=df.id.values, data_repository__hostname='ibl.flatironinstitute.org')
    fr = pd.DataFrame.from_records(frs.values(*filerecord_fields))
    df = df.set_index('id').join(fr.set_index('dataset_id'))
    df['file_path'] = df.pop('data_repository__globus_path').str.cat(df.pop('relative_path'))
    all_df.append(df)

df = pd.concat(all_df, ignore_index=False)
del all_df

for eid, rec in df.groupby('session'):
    logger.info(f'Updating session {eid}')
    dids = rec.index.values
    session_path = next(map(get_session_path, rec['file_path'].values))
    src_dir = root + session_path.as_posix()
    dst_dir = bucket_name.strip('/') + '/' + get_alf_path(src_dir)
    cmd = ['aws', 's3', 'sync', src_dir, dst_dir, '--delete', '--profile', 'miles']
    logger.debug(' '.join(cmd))
    t0 = time.time()
    result = subprocess.run(cmd)
    result.check_returncode()
    logger.debug(f'Sync took {(time.time() - t0) / 60:.2f}min')

    lab, *_, collection, revision = folder_parts(session_path)
    repo = f'aws_{lab}'
    for did, rec in rec.iterrows():
        record = {
            'dataset': Dataset.objects.get(id=did),
            'data_repository': DataRepository.objects.get(name=repo),
            'relative_path': rec['file_path'].replace(f'{lab}/Subjects', '').strip('/')
        }
        fr, is_new = FileRecord.objects.get_or_create(**record)
        fr.exists = Path(root + rec['file_path']).exists()
        fr.full_clean()
        fr.save()

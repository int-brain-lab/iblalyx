# %%
"""
--------------
1. TO RUN ON MBOX
--------------
"""
import sys
from pathlib import Path

import tqdm
import pandas as pd
import boto3

import ibl_reports.views
from django.db.models import Count, Q, Exists, OuterRef
from data.models import Dataset, FileRecord, DataRepository

IBL_ALYX_ROOT = Path(ibl_reports.views.__file__).resolve().parents[2]
sys.path.append(str(IBL_ALYX_ROOT.parent))
import iblalyx.releases.utils

# Make sure the releases you want to check are in here, you don't have to run this for all the old releases, though
# doesn't hurt to do it sometimes to make sure nothing has been lost.
# You can also just create a list of datasets that you want to double check on (specifiy datasets below and skip this)
public_ds_files = iblalyx.releases.utils.PUBLIC_DS_FILES

# Select which release you want to check by changing i
i = -1

# Load datasets and check if they have the FI and AWS file records and both exist
dset_file = IBL_ALYX_ROOT.joinpath('releases', public_ds_files[i])
datasets = Dataset.objects.filter(pk__in=list(pd.read_parquet(dset_file)['dataset_id']))
incorrect_repos = []
correct = []
not_exist = []


# Annotate datasets with counts of file records and existence flags
datasets = datasets.annotate(
    aws_count=Count('file_records', filter=Q(file_records__data_repository__name__startswith='aws')),
    flatiron_count=Count('file_records', filter=Q(file_records__data_repository__name__startswith='flatiron')),
    aws_exists=Exists(FileRecord.objects.filter(
        dataset_id=OuterRef('pk'),
        data_repository__name__startswith='aws',
        data_repository__globus_is_personal=False,
        exists=True
    )),
    flatiron_exists=Exists(FileRecord.objects.filter(
        dataset_id=OuterRef('pk'),
        data_repository__name__startswith='flatiron',
        data_repository__globus_is_personal=False,
        exists=True
    ))
)

incorrect_repos = datasets.filter(aws_count__gt=1, flatiron_count__gt=1)
missing_flatiron_repo = datasets.filter(flatiron_count=0)
missing_aws_repo = datasets.filter(aws_count=0)
aws_exists_false = datasets.filter(aws_count=1, aws_exists=False)
fi_exists_false = datasets.filter(flatiron_count=1, flatiron_exists=False)
correct = datasets.filter(aws_count=1, flatiron_count=1, aws_exists=True, flatiron_exists=True)

print(f'{public_ds_files[i]}: {datasets.count()} datasets')
print(f'Incorrect repository count (too many): {incorrect_repos.count()}')
print(f"AWS file record doesn't exist: {missing_aws_repo.count()}")
print(f"FI file record doesn't exist: {missing_flatiron_repo.count()}")
print(f"AWS file set to exists=False: {aws_exists_false.count()}")
print(f"FI file set to exists=False: {fi_exists_false.count()}")

print(f'Correct: {correct.count()}')

if correct.count() < datasets.count():
    df_errors = pd.DataFrame(datasets.exclude(aws_count=1, flatiron_count=1, aws_exists=True, flatiron_exists=True).values_list(
        'id', 'session__id', 'session__subject', 'collection', 'name', 'aws_count', 'flatiron_count', 'aws_exists', 'flatiron_exists'),
    columns=['id', 'eid', 'session__subject', 'collection', 'name', 'aws_count', 'flatiron_count', 'aws_exists', 'flatiron_exists'])


# %%
"""
---------------------------------------------------------------------------------------------------------
THE FOLLOWING THREE STEPS, ONLY IF THERE ARE ISSUES (INCORRECT REPOS OR FILE RECORDS THAT DO NOT EXIST)
otherwise skip to point 3
---------------------------------------------------------------------------------------------------------
"""

"""
FIXING missing_aws_repo datasets
2a. ON MBOX
"""

# If necessary create missing file records, set to exist=False to makes sure data gets transferred
# Check in dry run first and then set dry run to False if it all looks good
dry_run = True
for d in tqdm.tqdm(missing_aws_repo, total=missing_aws_repo.count()):
    frs = d.file_records.filter(data_repository__globus_is_personal=False)
    # Make sure it is that one and only one FR is missing, otherwise we probably need to do this manually
    assert frs.count() == 1
    lab = d.session.lab
    missing_repo = DataRepository.objects.filter(name__icontains=lab, globus_is_personal=False).exclude(
        name=frs[0].data_repository.name)
    assert missing_repo.count() == 1
    rel_path = frs[0].relative_path
    print('Creating', missing_repo[0].name, rel_path)
    if not dry_run:
        FileRecord.objects.create(dataset=d,
                                  relative_path=rel_path,
                                  data_repository=missing_repo[0],
                                  exists=False)
# then re-run the above queries to regenerate the list of datasets. Those missing_aws_repo should now be in the
# aws_exists_false queryset and you can then run the block below
# %%
"""
FIXING aws_exists_false datasets
2b. Generate a command to run ON SDSC
"""
# force sync the datasets that didn't have an aws file record before: first set the autodatetime field to now and
# then prepare the command to sync the datasets in one pass on SDSC
import datetime

update_time = datetime.datetime.now(datetime.timezone.utc)
aws_exists_false.update(auto_datetime=update_time.isoformat())
print(f"python manage.py update_aws --from-date {(update_time - datetime.timedelta(seconds=1)).isoformat()}")



# %%
"""
2c. ON MBOX
"""
# Once syncing on SDSC has finished, rerun the block above checking that all expected file records exist
# Finally check that these file records are not just set to exist, but the files indeed exist on AWS on SDSC
# (for SDSC see below, you can do these two in parallel)
aws_repo = DataRepository.objects.filter(name__startswith='aws').first()
session = boto3.Session()
s3 = boto3.resource('s3',
                    region_name=aws_repo.json['region_name'],
                    aws_access_key_id=aws_repo.json['Access key ID'],
                    aws_secret_access_key=aws_repo.json['Secret access key'])
bucket = s3.Bucket(name='ibl-brain-wide-map-private')

# Make sure you are still looking at the correct release
i = 10
dset_file = IBL_ALYX_ROOT.joinpath('releases', public_ds_files[i])
datasets = Dataset.objects.filter(pk__in=list(pd.read_parquet(dset_file)['dataset_id']))
fr = FileRecord.objects.filter(dataset__in=datasets, data_repository__name__startswith='aws')

exist = []
nonexist = []
for f in fr:
    lab = f.data_repository.name.split('_')[-1]
    if lab == 'ucla':
        lab = 'churchlandlab_ucla'
    dset_id = str(f.dataset.id)
    parts = f.relative_path.split('/')
    name = parts[-1].split('.')
    relative_path = '/'.join(parts[:-1]) + '/' + '.'.join(name[:-1] + [dset_id] + [name[-1]])
    found = next(iter(bucket.objects.filter(Prefix=f'data/{lab}/Subjects/{relative_path}')), False)
    if found is False:
        nonexist.append(f)
    else:
        exist.append(f)

print(f'{public_ds_files[i]}: {datasets.count()} datasets')
print(f'Does not exist on AWS: {len(nonexist)}')
print(f'Exists on AWS: {len(exist)}')


"""
--------------
3. TO RUN ON SDSC
--------------
"""
# Check that files also indeed exist on SDSC. Run this in django shell
from pathlib import Path
import pandas as pd
from one.alf.path import add_uuid_string
from data.models import DataRepository, Dataset

# Make sure iblalyx is up to date before running this!
IBL_ALYX_ROOT = Path('/home/datauser/Documents/PYTHON/iblalyx/')

# Make sure the releases you want to check are in here, never hurts to check the old releases too
public_ds_files = ['2021_Q1_IBL_et_al_Behaviour_datasets.pqt',
                   '2021_Q2_Varol_et_al_datasets.pqt',
                   '2021_Q3_Whiteway_et_al_datasets.pqt',
                   '2021_Q2_PreRelease_datasets.pqt',
                   '2022_Q2_IBL_et_al_RepeatedSite_datasets.pqt',
                   '2022_Q3_IBL_et_al_DAWG_datasets.pqt',
                   '2022_Q4_IBL_et_al_BWM_datasets.pqt',
                   '2023_Q1_Mohammadi_et_al_datasets.pqt',
                   '2023_Q1_Biderman_Whiteway_et_al_datasets.pqt',
                   '2023_Q3_Findling_Hubert_et_al_datasets.pqt',
                   '2023_Q4_Bruijns_et_al_datasets.pqt',
                   '2023_Q4_IBL_et_al_BWM_2_datasets.pqt',
                   '2023_Q4_IBL_et_al_BWM_passive_datasets.pqt',
                   '2024_Q2_IBL_et_al_BWM_iblsort_datasets.pqt',
                   '2024_Q2_IBL_et_al_RepeatedSite_datasets.pqt',
                   '2024_Q2_Blau_et_al_datasets.pqt',
                   '2024_Q3_Pan_Vazquez_et_al_datasets.pqt',
                   '2025_Q1_IBL_et_al_BWM_wheel_patch.pqt',
                   '2025_Q3_Zang_et_al_Aging.pqt',
                   ]

# Chose release with i
i = -1
dset_file = IBL_ALYX_ROOT.joinpath('releases', public_ds_files[i])
datasets = Dataset.objects.filter(pk__in=list(pd.read_parquet(dset_file)['dataset_id']))

no_record = []
nonexist = []
for dset in datasets:
    fr = dset.file_records.filter(data_repository__name__startswith='flatiron').first()
    if fr is None:
        no_record.append()
    else:
        rel_path = Path(fr.data_repository.globus_path).joinpath(fr.relative_path).relative_to('/')
        rel_path = add_uuid_string(str(rel_path), dset.pk)
        source = Path('/mnt/ibl').joinpath(rel_path)
        if not source.exists():
            nonexist.append(dset)

print(f'{public_ds_files[i]}: {datasets.count()} datasets')
print(f'No file record: {len(no_record)}')
print(f'Does not exist: {len(nonexist)}')

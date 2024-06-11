"""
--------------
1. TO RUN ON MBOX
--------------
"""

from data.models import Dataset, FileRecord, DataRepository
from pathlib import Path
import pandas as pd
import boto3
import ibl_reports.views

IBL_ALYX_ROOT = Path(ibl_reports.views.__file__).resolve().parents[2]

# Make sure the releases you want to check are in here, you don't have to run this for all the old releases, though
# doesn't hurt to do it sometimes to make sure nothing has been lost.
# You can also just create a list of datasets that you want to double check on (specifiy datasets below and skip this)
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
                   '2024_Q2_IBL_et_al_BWM_iblsort.pqt'
                   ]

# Select which release you want to check by changing i
i = 6

# Load datasets and check if they have the FI and AWS file records and both exist
dset_file = IBL_ALYX_ROOT.joinpath('releases', public_ds_files[i])
datasets = Dataset.objects.filter(pk__in=list(pd.read_parquet(dset_file)['dataset_id']))
incorrect_repos = []
correct = []
not_exist = []

for d in datasets:
    fr = d.file_records.filter(data_repository__globus_is_personal=False)
    if fr.count() == 2:
        repos = [f.data_repository.name.split('_')[0] for f in fr]
        if set(repos) == set(['aws', 'flatiron']):
            if all([f.exists for f in fr]):
                correct.append(d)
            else:
                not_exist.append(d)
        else:
            incorrect_repos.append(d)
    else:
        incorrect_repos.append(d)

print(f'{public_ds_files[i]}: {datasets.count()} datasets')
print(f'Incorrect repositories: {len(incorrect_repos)}')
print(f'File records exist=False: {len(not_exist)}')
print(f'Correct: {len(correct)}')

"""
---------------------------------------------------------------------------------------------------------
THE FOLLOWING THREE STEPS, ONLY IF THERE ARE ISSUES (INCORRECT REPOS OR FILE RECORDS THAT DO NOT EXIST)
otherwise skip to point 3
---------------------------------------------------------------------------------------------------------
"""

"""
2a. ON MBOX
"""

# Check that all the nonexistent file records are aws ones, so far this has been the case, if not, figure out what to do
for d in not_exist:
    for f in d.file_records.filter(data_repository__globus_is_personal=False):
        if not f.exists:
            if not f.data_repository.name.startswith('aws'):
                print(f.data_repository.name)

# If necessary create missing file records, set to exist=False to makes sure data gets transferred
# Check in dry run first and then set dry run to False if it all looks good
dry_run = True
for d in incorrect_repos:
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

"""
2b. ON SDSC
"""
# force sync the datasets that didn't have an aws file record before (write ids to txt file and scp over or copy paste)
to_check = [str(d.id) for d in not_exist]
with open(r'/home/ubuntu/to_check.txt', 'w') as fp:
    fp.write("\n".join(str(item) for item in to_check))

# Make a bash script with this content and run it
# #!/bin/bash
# fname='/home/datauser/to_check.txt'
# while read line; do
# echo $line
# python alyx/manage.py update_aws --dataset $line -v 2 &>> /home/datauser/Documents/github/alyx/tmp.log
# done < $fname



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
from one.alf.files import add_uuid_string
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
                   '2024_Q2_IBL_et_al_BWM_iblsort.pqt'
                   ]

# Chose release with i
i = 6
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
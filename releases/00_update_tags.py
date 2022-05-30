# this creates the tags on the main database before release
from data.models import Dataset, Tag
import pandas as pd
import ibl_reports.views
from pathlib import Path
IBL_ALYX_ROOT = Path(ibl_reports.views.__file__).resolve().parents[2]

"""
Settings and Inputs
the parquet files names match exactly the tag name in the database
"""
public_ds_files = ['2021_Q1_IBL_et_al_Behaviour_datasets.pqt',
                   '2021_Q2_Varol_et_al_datasets.pqt',
                   '2021_Q3_Whiteway_et_al_datasets.pqt',
                   '2021_Q2_PreRelease_datasets.pqt',
                   '2022_Q2_IBL_et_al_RepeatedSite_datasets.pqt',
                   ]
public_ds_tags = [
    "cfc4906a-316e-4150-8222-fe7e7f13bdac",  # "Behaviour Paper", "2021_Q1_IBL_et_al_Behaviour"
    "9dec1de8-389d-40f6-b00b-763e4fda6552",  # "Erdem's paper", "2021_Q2_Varol_et_al"
    "c8f0892a-a95b-4181-b8e6-d5d31cb97449",  # "Matt's paper", "2021_Q3_Whiteway_et_al"
    "dcd8b2e5-3a32-41b4-ac15-085a208a4466",  # "May 2021 pre-release", "2021_Q2_PreRelease"
    "05fbaa4e-681d-41c5-ae53-072cb96f4c0a",  # 2022_Q2_IBL_et_al_RepeatedSite_datasets
    ]


for pdn, tagid in zip(public_ds_files, public_ds_tags):
    pdf = IBL_ALYX_ROOT.joinpath('releases', pdn)
    tag = Tag.objects.get(id=tagid)
    datasets = Dataset.objects.filter(pk__in=list(pd.read_parquet(pdf)['dataset_id']))
    if set(tag.datasets.all()) == set(datasets):
        print(pdn, " all tags matching, skip")
    else:
        print(f"{pdn} update tags for {datasets.count()} datasets")
        tag.datasets.set(datasets)

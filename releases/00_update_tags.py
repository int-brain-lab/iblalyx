# %% this creates the tags on the main database before release - run on MBOX
import sys
from pathlib import Path

import pandas as pd

from data.models import Dataset, Tag
import ibl_reports.views

IBL_ALYX_ROOT = Path(ibl_reports.views.__file__).resolve().parents[2]
sys.path.append(str(IBL_ALYX_ROOT.parent))
import iblalyx.releases.utils

public_ds_files = iblalyx.releases.utils.PUBLIC_DS_FILES
public_ds_tags = iblalyx.releases.utils.PUBLIC_DS_TAGS

for pdn, tagid in zip(reversed(public_ds_files), reversed(public_ds_tags)):
    pdf = IBL_ALYX_ROOT.joinpath('releases', pdn)
    tag = Tag.objects.get(id=tagid)
    datasets = Dataset.objects.filter(pk__in=list(pd.read_parquet(pdf)['dataset_id']))
    if set(tag.datasets.all()) == set(datasets):
        print(pdn, " all tags matching, skip")
    else:
        print(f"{pdn} update tags for {datasets.count()} datasets")
        tag.datasets.set(datasets)

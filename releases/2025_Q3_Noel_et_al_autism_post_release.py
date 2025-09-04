# %%
from pathlib import Path
import alyx.base
import sys

import pandas as pd  # uv pip install openpyxl
import tqdm



df_datasets = pd.read_parquet('/home/olivier/PycharmProjects/alyx_ferret/iblalyx/releases/2025_Q3_Noel_et_al_Autism_datasets.pqt')


# %%
import numpy as np

df_sessions = df_datasets.groupby('eid').agg(
    n_trials=pd.NamedAgg(column='dataset_type', aggfunc=lambda x: np.sum(x.str.contains('trials.'))),
    has_table=pd.NamedAgg(column='dataset_type', aggfunc=lambda x: np.sum(x == 'trials.table')),
)

np.sum(df_sessions['has_table'] == 0)

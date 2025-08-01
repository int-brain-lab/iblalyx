"""

Other data repository: https://osf.io/fap2s/
Nature Neuroscience | Volume 28 | July 2025 | 1519–1532 1519 nature neuroscience
https://doi.org/10.1038/s41593-025-01965-8

https://osf.io/fap2s/wiki/home/

"""
# %%
from pathlib import Path
import alyx.base
import sys

import pandas as pd  # uv pip install openpyxl
import tqdm

from data.models import Dataset, Tag

IBL_ALYX_ROOT = Path(alyx.base.__file__).parents[3].joinpath('iblalyx')
assert IBL_ALYX_ROOT.exists(), 'No IBL_ALYX_ROOT found, it is usually at the same directory level as the alyx repo'
sys.path.append(str(IBL_ALYX_ROOT.parent))

import iblalyx.releases.utils


def parse_excel_files_to_eid_dataframe(path_xls):
    """
    JP has provided us with Excel files listing subjects and dates for each session. Unfortunately, there
    is no EID nor session number, which can yield to ambiguity in the selection of sessions.
    Here I chose to retain sessions with more than 42 trials.
    I did also annotate each EID with a flag indicating whether it is an ephys session or not

    On the run in July 2025, all the 238 ephys session yielded by the ONE query on the private database were included
    in the dataframe of eids generated from the Excel files.

    :return:
    """
    path_xls = Path('/home/olivier/scratch/autism') if path_xls is None else path_xls
    from one.api import ONE
    one = ONE()

    all_dfs = []
    for xls_file in path_xls.glob('*.xlsx'):
        df_xls = pd.read_excel(xls_file, header=None)
        df_xls = df_xls.iloc[:, [0, 2]]
        df_xls.columns = ['subject', 'date']
        df_xls['date'] = df_xls['date'].apply(lambda x: x[:10])
        df_xls['gene'] = xls_file.stem.split('_')[-1]

        for i, rec in tqdm.tqdm(df_xls.iterrows(), total=df_xls.shape[0]):
            rest_sessions = one.alyx.rest('sessions', 'list', subject=rec.subject, date_range=[rec.date, rec.date])
            if len(rest_sessions) > 1:
                print(f'Multiple sessions found for {rec.subject} {rec.date}')
                for rs in rest_sessions:
                    session_info = one.alyx.rest('sessions', 'read', id=rs['id'])
                    if session_info['n_trials'] is not None and session_info['n_trials'] > 42:
                        print(f'Includes session #{rs["number"]} with {session_info['n_trials']} trials')
                        all_dfs.append({
                            'eid': rs['id'],
                            'subject': rec.subject,
                            'date': rec.date,
                            'number': rs['number'],
                            'gene': rec.gene,
                        })
                    else:
                        print(f'Excludes session #{rs["number"]} with {session_info['n_trials']} trials')
            else:
                all_dfs.append({
                    'eid': rest_sessions[0]['id'],
                    'subject': rec.subject,
                    'date': rec.date,
                    'number': rest_sessions[0]['number'],
                    'gene': rec.gene,
                })

    df_sessions_autism = pd.DataFrame(all_dfs)
    df_sessions_autism.set_index('eid', inplace=True)
    df_sessions_autism['ephys'] = False

    project = "angelaki_mouseASD"
    # Get all insertions for this project
    str_query = (
        f"session__projects__name__icontains,{project},"
        "session__qc__lt,50,"
        "~json__qc,CRITICAL"
    )
    insertions = one.alyx.rest("insertions", "list", django=str_query)
    eids_ephys = [ins['session'] for ins in insertions]

    df_sessions_autism.loc[eids_ephys, 'ephys'] = True
    return df_sessions_autism


# %% Start of the main script
TAG_NAME = '2025_Q3_Noel_et_al_Autism'
DRY_RUN = True

# read in the eid list according to what JP has released in the Excel files
file_eids = IBL_ALYX_ROOT.joinpath('releases', '2025_Q3_Noel_et_al_Autism_EIDS.pqt')
if file_eids.exists():
    df_eids = pd.read_parquet(file_eids)
else:
    df_eids = parse_excel_files_to_eid_dataframe()
    df_eids.to_parquet(file_eids)

eids_ephys = df_eids[df_eids['ephys']].index.tolist()
eids = df_eids.index.tolist()

df_datasets = []

# behaviour and wheel datasets
dsets = Dataset.objects.filter(session__in=eids, dataset_type__name__in=iblalyx.releases.utils.DTYPES_RELEASE_BEHAVIOUR)
df_datasets.append(pd.DataFrame([str(eid) for eid in dsets.values_list('pk', flat=True)], columns=['dataset_id']))


# %% for ephys sessions, we get video and ephys datasets

# video datasets only for ephys sessions: we exlude QC critical datasets and include lick times
dsets = iblalyx.releases.utils.get_video_datasets_for_ephys_sessions(eids_ephys, cam_labels=['left', 'right', 'body'])
df_datasets.append(pd.DataFrame([str(eid) for eid in dsets.values_list('pk', flat=True)], columns=['dataset_id']))

# ephys datasets
dsets = Dataset.objects.filter(session__in=eids_ephys, dataset_type__name__in=iblalyx.releases.utils.DTYPES_RELEASE_EPHYS_ALL)
df_datasets.append(pd.DataFrame([str(eid) for eid in dsets.values_list('pk', flat=True)], columns=['dataset_id']))

# finalize
df_datasets = pd.concat(df_datasets, axis=1)


# %% Tagging in production database
if DRY_RUN is False:
    dsets2tag = Dataset.objects.filter(id__in=df_datasets['dataset_id'])
    tag, _ = Tag.objects.get_or_create(name=TAG_NAME, protected=True, public=True)
    tag.datasets.set(dsets2tag)

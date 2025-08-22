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

# those eids where exluded as impossible to read bpod trace in the sync stream of the FPGA - see release notes
EXCLUDES = [
    '429de7e8-1fc9-4aa2-87a1-0800268935d7',  # 4350
    '5d4e158b-7d6d-48fd-ad94-74ad4704e89f',  # 4351
    '8507e9f6-4da3-454a-b553-3fc8f6299bbb',  # 4354
    'e9620e9a-688a-45da-ba6e-33fce6753729',  # 4355
]

def parse_excel_files_to_eid_dataframe(path_xls=None):
    """
    JP has provided us with Excel files listing subjects and dates for each session. Unfortunately, there
    is no EID nor session number, which can yield to ambiguity in the selection of sessions.
    Here I chose to retain sessions with more than 42 trials.
    I did also annotate each EID with a flag indicating whether it is an ephys session or not

    On the run in July 2025, all the 238 ephys session yielded by the ONE query on the private database were included
    in the dataframe of eids generated from the Excel files.

    :return:
    """
    path_xls = Path('/datadisk/Data/2025/08_autism_release/jp_spreadsheets') if path_xls is None else path_xls
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
file_eids = IBL_ALYX_ROOT.joinpath('releases', '2025_Q3_Noel_et_al_Autism_eids.pqt')
if file_eids.exists():
    df_eids = pd.read_parquet(file_eids)
else:
    df_eids = parse_excel_files_to_eid_dataframe()
    df_eids.to_parquet(file_eids)
# drop excluded eids - see release notes
df_eids = df_eids.drop(index=EXCLUDES)

eids_ephys = df_eids[df_eids['ephys']].index.tolist()
eids = df_eids.index.tolist()


# %% Get behaviour and wheel datasets
df_datasets = []

# behaviour and wheel datasets
dsets = Dataset.objects.filter(session__in=eids, dataset_type__name__in=iblalyx.releases.utils.DTYPES_RELEASE_BEHAVIOUR)
df_datasets.append(iblalyx.releases.utils.dset2df(dsets))


# %% for ephys sessions, we get video and ephys datasets

# video datasets only for ephys sessions: we exlude QC critical datasets and include lick times
dsets = iblalyx.releases.utils.get_video_datasets_for_ephys_sessions(eids_ephys, cam_labels=['left', 'right', 'body'])
df_datasets.append(iblalyx.releases.utils.dset2df(dsets))

# ephys datasets
dsets = Dataset.objects.filter(session__in=eids_ephys, dataset_type__name__in=iblalyx.releases.utils.DTYPES_RELEASE_EPHYS_ALL)
df_datasets.append(iblalyx.releases.utils.dset2df(dsets))

# finalize
df_datasets = pd.concat(df_datasets, axis=0).reset_index(drop=True)
df_datasets.to_parquet(IBL_ALYX_ROOT.joinpath('releases', f'{TAG_NAME}.pqt'))


# %% Tagging in production database
if DRY_RUN is False:
    dsets2tag = Dataset.objects.filter(id__in=df_datasets['dataset_id'])
    tag, _ = Tag.objects.get_or_create(name=TAG_NAME, protected=True, public=True)
    tag.datasets.set(dsets2tag)

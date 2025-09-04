# %%
from pathlib import Path
import alyx.base
import sys

import pandas as pd  # uv pip install openpyxl
import tqdm

from data.models import Dataset, Tag
from experiments.models import ProbeInsertion

IBL_ALYX_ROOT = Path(alyx.base.__file__).parents[3].joinpath('iblalyx')
assert IBL_ALYX_ROOT.exists(), 'No IBL_ALYX_ROOT found, it is usually at the same directory level as the alyx repo'
sys.path.append(str(IBL_ALYX_ROOT.parent))

import iblalyx.releases.utils

TAG_NAME = '2025_Q3_Noel_et_al_Autism'
DRY_RUN = True

EID_EXCLUDES = [ # those are sessions for which there is no behaviour data available
    '11cdb19a-6c13-41e7-b989-a2ad1f829c8b',
    '429de7e8-1fc9-4aa2-87a1-0800268935d7',  # no bpod in FPGA
    '59642ec6-44a3-467d-a20f-19b5a29414b7',
    '5d4e158b-7d6d-48fd-ad94-74ad4704e89f',  # no bpod in FPGA
    '6996f5c7-32ad-4007-aec4-b016364b0b7e',
    '7dc5595a-13c3-4fcc-a607-dab1d3eba4a5',
    '8507e9f6-4da3-454a-b553-3fc8f6299bbb',  # no bpod in FPGA
    '87f8b50e-194f-46f5-87d7-4113a9ec5cb7',
    '8def5ec1-f83c-4367-ae59-bb066d44c7bb',
    'dce99574-223b-4b99-825c-65753e47441f',
    'e9620e9a-688a-45da-ba6e-33fce6753729'  # no bpod in FPGA
]

# PIDS exclude reason
#                            364
# traced but no alignment     49
# no spike sorting            36
# no tracing                  17

PID_EXCLUDES = [  # those pids do not have spike sorting
    '1034a9c5-85cd-49c1-bf6b-0fb2cbc4ee67',
    '131716c1-515e-4a45-9158-cf1af6da39c7',
    '1606f00b-e4a0-434e-a81b-a492016b42d9',
    '1b6ee150-1c07-4bd9-a37a-33a23ef2e982',
    '27c9bbd4-4d14-43f7-a403-8ee251d93db9',
    '2a32115c-bb3e-42ec-81a4-c24f11d1721f',
    '32a40ddf-34fc-457e-899f-cc60a7f9d009',
    '3edd5b47-496e-44c4-a031-8ee2621edde4',
    '474965f4-5751-497b-9423-cb130fc30644',
    '4b8d4b2c-4184-4175-8013-cf03ff176458',
    '4c35cc14-8930-418a-b202-f6fcda9d14b2',
    '5e95bef4-0545-4ec3-a97d-251f7b76bfe8',
    '64e04be3-3ab1-4858-bd2d-8254729074dd',
    '65c6349d-6ec3-4a7d-aefc-7cfe328c1faf',
    '7b00f29f-f67f-4fa9-b4c9-e844c44b7b6b',
    '7bf6ff8d-6487-481e-a174-d1cbe31db2c4',
    '873073f2-752d-4fdf-b350-aeb5afd93ff3',
    '8ade2696-5a06-4e0e-8f73-674aaf60a6ca',
    '8b326db7-e6ac-483e-aea3-2d7532c0a74a',
    '8dd574fc-6b20-4ad2-b0d7-3418c3a831d5',
    '9a51e010-4c21-4c97-9ba1-faa4dabdf08b',
    'a25d9370-a714-4662-88c5-ccf69b646bd5',
    'ae2d5893-d4f9-4f63-abfa-829cae227606',
    'bcb1dac7-6d2b-47ad-bbbe-a4aaf9774481',
    'c18a4246-6f73-4e09-b5ea-2ad9854cc3eb',
    'c2ef74c0-7750-476a-9c1a-ed914b0cf6db',
    'c31e3510-cc05-4992-ac30-808d3b3f0d81',
    'c7a1a6ff-6462-4877-b352-878be8c75927',
    'd622372d-f866-424b-920b-86bfab3f6c5e',
    'd8e48105-d993-4315-9c22-8080a59b184e',
    'de7639c4-0f81-4370-84f2-70dde255eca7',
    'e559ba5c-07f4-4b0b-bfa4-2c7979f9d872',
    'eddf08b2-2b1f-489c-bceb-f28592518f61',
    'ef8eb985-3731-48c5-ac62-38214f8d8ee4',
    'f478ab5d-d895-423d-93b0-528dbc135272',
    'f8b4d35c-54dd-489b-9d12-660807e7bdf2'
]

PID_EXCLUDES += [  # those pids have no tracing nor histology
    '25d257a3-ef91-470f-8e33-c1b55daea7a2',
    '2eeeee9a-678e-47c9-b2d9-22daec55ddbb',
    '2fe497f6-95e8-4349-9b92-47f26265b784',
    '553b258e-e21d-48b2-8065-21246c82e51a',
    '5831925c-4b27-425f-9b7a-ff8b0691e9f3',
    '7904a078-ff02-4e47-b646-01bffdb19d28',
    '810a4c3e-a5fa-43a6-869b-5d50e236e0e8',
    '85e1543c-aa2d-4a86-b933-800431204d3d',
    'ada946d0-1195-4f04-9d45-8e7e6adf7f60',
    'bd38374f-06d6-4cc5-8b51-da5dda4afd5a',
    'be8f5333-ea97-4a80-8574-ab937b2087cd',
    'c80254e4-9457-4310-bae1-e2cbee38ab75',
    'd463910a-3c5d-46b0-b604-7a4f329116a2',
    'd5005f88-c01f-417c-bbf2-dee7a58a1984',
    'd8ed8bbe-c2fc-46ad-b769-872d326a8179',
    'de1fd61e-ab02-4fe1-ad89-981f532075bf',
    'dff35de0-1906-4f40-bc00-bc3031aed7c4'
 ]

PID_EXCLUDES += [  # those pids have a tracing but no alignment availabel
    '051a6c5f-4a75-42e2-aa82-b4e3cbe1cecf',
    '05b7f762-7496-4d80-a83f-da269a015bd6',
    '0b91325f-0271-468d-a3e6-edab366af9ae',
    '0bd312b8-e085-4483-81a5-7bada5c1e844',
    '131e4225-f661-4d2a-9e82-ce5c476ca33c',
    '14ff1447-287a-4b13-9b2a-b3580345f7a4',
    '15023776-7eb0-4563-ba0e-609c1d917889',
    '165cadc9-4faa-4b44-9fd7-595227661623',
    '26703f1e-64dd-43d6-b86a-1c624e6536ae',
    '27271de8-f1ff-4299-a547-05f1be477417',
    '2793cec9-4422-47b4-a58c-4dc5a71baa41',
    '2b5bf678-e42d-4007-900f-b07b4388d4e3',
    '326c22d1-dbce-41dd-92f7-26d6e4a8d9dc',
    '3283f10e-4534-48cb-938d-2d44ab4eae94',
    '3b506a8b-ea2a-4bc1-89d6-a3b0d9644744',
    '3d6b124f-3781-4e40-9526-12e49acdcbf7',
    '3edb28e2-7fdc-4550-8fe6-7f6edf01aba0',
    '57caac44-1713-4650-beaf-8ad81132171a',
    '58c392e4-1a2e-455e-89eb-65a3ceab8093',
    '66887465-3ace-43d6-b609-5e5e1878e8bd',
    '685fb0f1-ed10-4a8b-8e10-51b42a7d67eb',
    '6f74c301-8d73-49c3-b2fd-6ad1a74528ce',
    '80861a35-b6eb-4362-94f4-f80d44863b55',
    '8700df71-6d4b-41bd-9907-e2b2396d06ad',
    '8d6c9ffc-6606-4f11-89bf-cc422ce5022a',
    '8e5c51e1-690d-4b50-88c4-37242d5ccb65',
    '94d1359a-3e87-40a5-b747-7e2b85a4330a',
    '9d951578-3e86-4ecd-8bf8-673ffb627f5a',
    '9dd7e4a7-391a-4f61-982f-0efa470ccf59',
    'a23e8b8c-0fa4-4074-86bf-c1835e4c48d1',
    'a695bd70-08f3-475d-89c6-e4c3db421374',
    'a6b0d2db-c3fc-4967-bdbf-e3f338f3af5e',
    'af95a0e6-073e-416c-9209-7ab30da8ce02',
    'b0bf5884-f2c5-461c-8f4d-09d21814fa73',
    'b84073a7-d367-4f85-8a27-4aba478a7ff6',
    'b8517cd0-1b2d-4e70-9758-d4696f3df8bb',
    'bc812e54-e20a-458a-a8eb-8836e351b701',
    'bfe18764-49f2-4f4f-b09e-caef0f8d59eb',
    'c59520cf-d52c-4bf2-90cc-e6924ab7bf2b',
    'c953e71f-c1d5-464c-9906-88438e957cad',
    'c9eb42ff-3d53-4044-9468-93e89d870368',
    'd805f64c-87dc-459c-b23d-8970483c3127',
    'de4b8038-c3bc-42cf-ad54-16b8c0fab0f9',
    'e3e2881e-a0a0-4896-b1cf-224762457325',
    'eacf11c1-47d5-4245-b715-0acc29ccec5c',
    'f1b199b0-561c-41c2-bbdd-341f29c6dc97',
    'f4fb1053-61a7-482a-9099-39ed436dd756',
    'fab06f3a-9ff5-405d-957c-6e628c25af3d',
    'fcdc2472-1025-41ef-bc0f-2c026c35dd6e'
 ]


def make_eid_dataframe(path_xls=None):
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


def make_pid_dataframe(eids_ephys):
    # this will need ibllib installed to work properly
    from one.api import ONE
    from brainbox.io.one import SpikeSortingLoader
    one = ONE()

    insertions = ProbeInsertion.objects.filter(session__in=eids_ephys, session__qc__lt=50).exclude(
        json__qc='CRITICAL')

    df_pids = pd.DataFrame(insertions.values_list('id', 'session', 'name', 'session__subject__nickname', 'session__start_time__date', 'session__number'),
                 columns=['pid', 'eid', 'pname','subject', 'date', 'number' ])
    df_pids['eid'] = df_pids['eid'].astype(str)
    df_pids['pid'] = df_pids['pid'].astype(str)
    df_pids.set_index('pid', inplace=True)

    df_pids['histology'] = 'UNKNOWN'
    for pid, rec in tqdm.tqdm(df_pids.iterrows(), total=df_pids.shape[0]):
        ssl = SpikeSortingLoader(pid=pid, one=one)
        try:
            ssl.load_channels()
            # spikes, clusters, channels = ssl.load_spike_sorting()
        except:
            ssl.histology = 'ERROR'
        df_pids.at[pid, 'histology'] = ssl.histology
    return df_pids


# %% Prepare the queries that give us the tables of sessions and insertions
# read in the eid list according to what JP has released in the Excel files
file_eids = IBL_ALYX_ROOT.joinpath('releases', '2025_Q3_Noel_et_al_Autism_eids.pqt')
file_pids = IBL_ALYX_ROOT.joinpath('releases', '2025_Q3_Noel_et_al_Autism_pids.pqt')

if file_eids.exists():
    df_eids = pd.read_parquet(file_eids)
else:
    df_eids = make_eid_dataframe()
    df_eids.to_parquet(file_eids)

if file_pids.exists():
    df_pids = pd.read_parquet(file_pids)
else:
    df_pids = make_pid_dataframe(list(df_eids.index[df_eids['ephys']].values))
    df_pids.to_parquet(file_pids)

df_pids['exclude'] = df_pids.index.map(lambda x: x in PID_EXCLUDES)
df_ephys_eids = df_pids.groupby('eid').agg(
    count=pd.NamedAgg(column="exclude", aggfunc="count"),
    exclude=pd.NamedAgg(column="exclude", aggfunc="mean")
)

# drop excluded eids - see release notes
EID_EXCLUDES = list(set(EID_EXCLUDES + list(df_ephys_eids.index[df_ephys_eids['exclude'] >= 1])))
print(f"{df_eids.shape[0]} eids before exclusion")
df_eids = df_eids.drop(index=EID_EXCLUDES)
print(f"{df_eids.shape[0]} eids after exclusion")


print(f"{df_pids.shape[0]} pids before exclusion")
df_pids = df_pids.drop(index=PID_EXCLUDES)
print(f"{df_pids.shape[0]} pids after exclusion")

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


# ephys datasets: we release only datasets from non exluded insertions
for pid, rec in df_pids.iterrows():
    dsets = Dataset.objects.filter(session=rec.eid, dataset_type__name__in=iblalyx.releases.utils.DTYPES_RELEASE_EPHYS_ALL, collection__icontains=rec.pname)
    df_datasets.append(iblalyx.releases.utils.dset2df(dsets))


# finalize
df_datasets = pd.concat(df_datasets, axis=0).reset_index(drop=True)
df_datasets.to_parquet(IBL_ALYX_ROOT.joinpath('releases', f'{TAG_NAME}.pqt'))

# %% Tagging in production database
if DRY_RUN is False:
    dsets2tag = Dataset.objects.filter(id__in=df_datasets['dataset_id'])
    tag, _ = Tag.objects.get_or_create(name=TAG_NAME, protected=True, public=True)
    tag.datasets.set(dsets2tag)


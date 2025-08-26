from pathlib import Path
import tqdm
from one.api import ONE

import pandas as pd
import numpy as np
import json
import neuropixel
import iblatlas.atlas
from iblutil.numerical import ismember

import ibllib.qc.alignment_qc
from ibllib.pipes.base_tasks import EphysTask
from brainbox.io.one import SessionLoader, EphysSessionLoader
from brainbox.io.one import SpikeSortingLoader

one = ONE()

EXCLUDES = [
    '429de7e8-1fc9-4aa2-87a1-0800268935d7',  # 4350
    '5d4e158b-7d6d-48fd-ad94-74ad4704e89f',  # 4351
    '8507e9f6-4da3-454a-b553-3fc8f6299bbb',  # 4354
    'e9620e9a-688a-45da-ba6e-33fce6753729',  # 4355
]
df_eids = pd.read_parquet(
    '/home/olivier/PycharmProjects/alyx_ferret/iblalyx/releases/2025_Q3_Noel_et_al_Autism_eids.pqt')  # 4356 sessions
df_eids = df_eids.drop(index=EXCLUDES)

eids_ephys = df_eids[df_eids['ephys']].index.tolist()
eids = df_eids.index.tolist()

# %% First check the loading af all behaviour sessions
#  388 1ba5bfae-ac10-4d3c-bf54-7d1082439302 /datadisk/FlatIron/angelakilab/Subjects/SH008/2020-03-02/001
# 2090 21ed8271-9d4d-4b05-bc5f-2fc4c0a9bb7c /datadisk/FlatIron/angelakilab/Subjects/SH012/2019-11-12/001
# 4350 429de7e8-1fc9-4aa2-87a1-0800268935d7 /datadisk/FlatIron/angelakilab/Subjects/CSP023/2020-11-17/001 ALF object not found
# 4351 5d4e158b-7d6d-48fd-ad94-74ad4704e89f /datadisk/FlatIron/angelakilab/Subjects/CSP018/2021-01-20/001
# 4352 1a4479ec-8511-48c9-9169-55a20f2626f9 /datadisk/FlatIron/angelakilab/Subjects/FMR031/2022-03-22/001
# 4353 baab8aae-273e-4f83-80a9-81e9b7f99185 /datadisk/FlatIron/angelakilab/Subjects/FMR032/2022-03-15/001
# 4354 8507e9f6-4da3-454a-b553-3fc8f6299bbb /datadisk/FlatIron/angelakilab/Subjects/SH014/2020-07-13/001
# 4355 e9620e9a-688a-45da-ba6e-33fce6753729 /datadisk/FlatIron/angelakilab/Subjects/SH008/2020-03-04/001

IMIN = 0
for i, eid in tqdm.tqdm(enumerate(eids)):
    if i < IMIN:
        continue
    print(i, eid, one.eid2path(eid))
    sl = SessionLoader(one=one, eid=eid)
    sl.load_trials()

# %% Next check the alignment status and the loading of spike sorting
import joblib


def get_histology(eid):
    esl = EphysSessionLoader(one=one, eid=eid)
    for pname in esl.ephys:
        ssl = esl.ephys[pname]['ssl']
        try:
            ssl.load_channels()
            # spikes, clusters, channels = ssl.load_spike_sorting()
        except:
            ssl.histology = 'ERROR'

        session_path = one.eid2path(eid)
        subject, date, number = session_path.parts[-3:]
        # NB: the old alignments are not necessary as they're a subset of the current alignments files
        file_align = next(Path('/datadisk/Data/2025/08_autism_release/alignments/channel_locations').glob(
            f'{subject}_{date}_{number}_{ssl.pname}_*.json'), '')
        # old_align = next(Path('/datadisk/Data/2025/08_autism_release/alignments/previous').glob(f'{subject}_{date}_{number}_{ssl.pname}_*.json'), '')
        pid_info = {'pid': ssl.pid, 'eid': eid, 'pname': pname, 'histology': ssl.histology,
                    'file_align': str(file_align), 'subject': subject, 'date': date, 'number': number}
    return pid_info


#
file_pids = Path('/datadisk/Data/2025/08_autism_release/pids_autism_release.pqt')
if not file_pids.exists():
    jobs = (joblib.delayed(get_histology)(eid) for eid in eids_ephys)
    df_pids = list(tqdm.tqdm(joblib.Parallel(return_as='generator', n_jobs=4)(jobs), total=len(eids_ephys)))
    df_pids = pd.DataFrame(df_pids)
    df_pids.set_index('pid', inplace=True)
    df_pids.to_parquet(file_pids)
else:
    df_pids = pd.read_parquet(file_pids)


# %%

def _interpolate_jp_missing_channels(file_align):
    # read in the json file
    with open(file_align, 'r') as f:
        data = json.load(f)
    df_channels_json = pd.DataFrame([data[k] for k in data.keys() if k.startswith('channel_')])

    th = neuropixel.trace_header(version=1)
    channels_xy = np.c_[th['x'], th['y']].astype(np.float32)
    nc = channels_xy.shape[0]

    json_xy = df_channels_json.loc[:, ['lateral', 'axial']].to_numpy()
    # for some reason JPs datasets don't have the right x local coordinate
    json_xy[np.mod(json_xy[:, 1], 40) == 0, 0] = json_xy[np.mod(json_xy[:, 1], 40) == 0, 0] + 16

    _, iall, ijson = np.intersect1d(channels_xy @ np.array([1, 1e5]), json_xy @ np.array([1, 1e5]), return_indices=True)

    channels_mlapdv = np.zeros([nc, 3]) * np.nan
    channels_mlapdv[iall, :] = df_channels_json.loc[:, ['x', 'y', 'z']].to_numpy()[ijson]

    # indices to interpolate
    ii = np.isnan(channels_mlapdv[:, 0])
    channels_mlapdv[ii, 0] = np.interp(channels_xy[ii, 1], channels_xy[~ii, 1], channels_mlapdv[~ii, 0])
    channels_mlapdv[ii, 1] = np.interp(channels_xy[ii, 1], channels_xy[~ii, 1], channels_mlapdv[~ii, 1])
    channels_mlapdv[ii, 2] = np.interp(channels_xy[ii, 1], channels_xy[~ii, 1], channels_mlapdv[~ii, 2])

    channels_atlas_id = ba.get_labels(channels_mlapdv / 1e6)
    np.testing.assert_array_equal(0, channels_atlas_id[iall] - df_channels_json['brain_region_id'].values[ijson])
    return channels_mlapdv, channels_atlas_id, channels_xy


def make_datasets_from_json(file_align, pid):
    eid, pname = one.pid2eid(pid)
    session_path = one.eid2path(eid)
    probe_path = session_path.joinpath('alf', pname)
    probe_path.mkdir(exist_ok=True, parents=True)
    channels_mlapdv, channels_atlas_id, channels_xy = _interpolate_jp_missing_channels(file_align)

    channels_mlapdv = channels_mlapdv.astype(np.int32)
    channels_atlas_id = channels_atlas_id.astype(int)
    channels_xy = channels_xy.astype(np.float32)

    files_to_register = []

    f_name = probe_path.joinpath('electrodeSites.mlapdv.npy')
    np.save(f_name, channels_mlapdv)
    files_to_register.append(f_name)

    f_name = probe_path.joinpath('electrodeSites.brainLocationIds_ccf_2017.npy')
    np.save(f_name, channels_atlas_id)
    files_to_register.append(f_name)

    f_name = probe_path.joinpath('electrodeSites.localCoordinates.npy')
    np.save(f_name, channels_xy)
    files_to_register.append(f_name)
    probe_collections = one.list_collections(eid, filename='channels*', collection=f'alf/{pname}*')
    for collection in probe_collections:
        channels_xy_local = one.load_dataset(eid, 'channels.localCoordinates', collection=collection)
        channels_xy_local[np.mod(channels_xy_local[:, 1], 40) == 0, 0] = channels_xy_local[
                                                                             np.mod(channels_xy_local[:, 1],
                                                                                    40) == 0, 0] + 16
        _, iall = ismember(channels_xy_local @ np.array([1, 1e5]), channels_xy @ np.array([1, 1e5]))

        f_name = session_path.joinpath(collection, 'channels.mlapdv.npy')
        np.save(f_name, channels_mlapdv[iall])
        files_to_register.append(f_name)

        f_name = session_path.joinpath(collection, 'channels.brainLocationIds_ccf_2017.npy')
        np.save(f_name, channels_atlas_id[iall])
        files_to_register.append(f_name)
    return files_to_register


class ProbeRegisterTaskA(EphysTask):

    def _run(self):
        aq = ibllib.qc.alignment_qc.AlignmentQC(one=one, probe_id=pid)
        aq.compute()
        aq.resolve_manual(align_key=aq.align_keys_sorted[-1], update=True, upload_alyx=True, upload_flatiron=True,
                          force=False)
        self.outputs = aq.create_electrode_datasets(alignment_key=aq.align_keys_sorted[-1])


class ProbeRegisterTaskB(EphysTask):

    def _run(self):
        self.outputs = make_datasets_from_json(file_align=rec['file_align'], pid=pid)


def register_histology(one, pid, TaskClass):
    eid, pname = one.pid2eid(pid)
    task = TaskClass(one=one, pname=pname, session_path=one.eid2path(eid), location='EC2')
    task.pid = pid
    task.run()
    task.register_datasets(labs='angelakilab', force=True)


# %% MAIN
# %% TODO: exclude those PIDs. In case this concerns all insertions for a session, exclude the whole session
df_pids['exclude'] = 0

print('| PID | EID | Subject | Date | Number | Probe |')
print('| --- | --- | --- | --- | --- | --- |')
for pid, rec in df_pids.iterrows():
    if rec['histology'] == 'ERROR':
        df_pids.loc[pid, 'exclude'] = 1
    elif rec['histology'] == 'aligned':
        if rec['file_align'] == '':
            """
            # Case A :all have alignments in Alyx, finalize alignments (just re-use code for alignments)
            | ad2670ab-e095-44a5-8892-dd844232ea5c | badc7140-b917-44a2-aa48-0e44f357baee | CSP004 | 2019-11-26 | 001  | probe00 |
            | 7754b3b0-d133-4cde-a416-4ca0385e548e | 279fa50f-223c-43ac-ac0c-89753c77949e | CSP004 | 2019-11-27 | 002  | probe00 |
            """
            # register_histology(one, pid, ProbeRegisterTaskA)
            pass
        else:
            """
            # Case B: create and register electrode locations from channel locations file provided by JP
            | f32a7ceb-cf66-4cac-be67-a0e99b1a47d6 | 4c53f746-7763-478d-b251-5315c26c4b5f | CSP004 | 2019-11-27 | 001  | probe00 |
            """
            # register_histology(one, pid, ProbeRegisterTaskB)
    elif rec['histology'] == 'traced':
        if rec['file_align'] == '':
            # All have tracing in Alyx, we exclude them anyways. An alternative would be to create and register electrode locations with a big warning
            df_pids.loc[pid, 'exclude'] = 1
            print(f'| {pid} | {rec.eid} | {rec.subject} | {rec.date} | {rec.number}  | {rec.pname} |')
            # print(one.alyx.rest('insertions', 'list', id=pid)[0]['json']['extended_qc'])
            pass
        else:
            # print(one.alyx.rest('insertions', 'list', id=pid)[0]['json']['extended_qc'])
            """
            # Case B: create and register electrode locations from channel locations file provided by JP
            | 05549272-e867-4a2b-9e8b-fad0bca8c504 | bc84df5d-7dec-441f-9eb0-f6b23deed715 | FMR010 | 2021-01-26 | 001  | probe01 |
            | f5b89f3f-b52d-41b6-a4b6-cfc15ac1d9c9 | b16f1259-b3a2-48be-bd24-ea827e6ab195 | FMR010 | 2021-01-27 | 001  | probe01 |
            | fa315ab1-fc22-4833-b9de-d358c2411bbe | 81161ed2-909d-4d3f-8c05-4c5dd3d22bdc | FMR010 | 2021-01-28 | 001  | probe01 |
            | 02165764-e0a1-46dd-a330-6c704774bb7b | 5ef0bd40-6c0a-46ed-bb62-dc407d9fcd09 | FMR010 | 2021-01-29 | 001  | probe00 |
            | 512de92b-56ca-41e8-89bb-c0d319397ac3 | 0cfcfb55-258f-4dc7-943c-30fbf48d1410 | FMR019 | 2021-05-26 | 001  | probe00 |
            | 7ed811d3-ac52-4600-9740-71489ae6bcbc | aa6e6db8-b57a-4b11-b1b9-b2f710af9598 | FMR019 | 2021-05-27 | 001  | probe00 |
            | f9b7cbfe-ab8b-417c-8cc3-47d1e55f97c2 | 80d617ee-4010-4be1-8b11-03e508e88dfa | FMR019 | 2021-05-28 | 001  | probe00 |
            | 0c78d3c8-9d59-4cc5-b12b-a4fb0200062e | 2fdc9b86-a1c6-483a-acbf-1dd97e264ef8 | SH002 | 2019-11-26 | 003  | probe01 |
            | e602f97c-2974-4316-8409-c64c21ea3239 | 50ddfe75-0337-4ecb-8a05-fac65ebf3a12 | SH012 | 2020-01-30 | 001  | probe01 |
            | c8635eee-b1c8-4a4e-a731-a0ec71a96498 | ef8f1e97-ccab-4e21-bd66-2d9ec4313db5 | SH015 | 2020-08-05 | 002  | probe01 |
            | 39013088-1388-4ee5-ae4b-7c8f9710d2c6 | bad413bd-6e5a-4532-937d-eeefa6b08515 | CSP018 | 2021-01-21 | 001  | probe00 |
            | 913b9aeb-329e-4e88-b9bd-594843f9e898 | 0ad4fa8e-8855-4539-841d-373d8c975bf6 | CSP018 | 2021-01-22 | 001  | probe00 |
            | a49ad9a8-1665-4659-a203-e8a3803b0bcf | 0574827a-c1f1-4cc0-8188-520a6e575c01 | CSP025 | 2021-06-01 | 001  | probe00 |
            | 662ccff2-8ce2-4ab5-97ae-440e9c243912 | fc8d9a41-5c68-435b-8ec3-a603b742d901 | CSP025 | 2021-06-02 | 001  | probe00 |
            | da7fd5e3-3332-4314-8f4e-5af09a517ad8 | 709b3f53-d379-43f3-822c-1e9bd26f38c6 | CSP025 | 2021-06-03 | 001  | probe00 |
            | f6c20b85-3c12-4e2b-9996-cd3cd5695bb1 | bfc87524-c333-4110-a52d-22530cee481b | CSP025 | 2021-06-04 | 001  | probe00 |
            | 321c8f10-97df-4e65-b561-e9efcf4e8609 | 9c1df22e-e9ae-4869-852d-fc93d7e89878 | CSP033 | 2022-02-08 | 002  | probe01 |
            | ca71f020-0649-465f-bd6b-d514f435fde9 | 67b1d2b6-eba8-4492-a766-ce6791538242 | NYU-32 | 2020-12-22 | 001  | probe00 |
            | ab41e289-eacb-42a7-b34b-44cd76b5cea8 | 03b83d58-23d9-467e-bac1-e12ab34683c0 | NYU-49 | 2021-07-21 | 001  | probe00 |
            | edd8c08d-726b-46e7-980e-69043ab87e5f | 3dad6235-17ae-4eb3-bcb8-8fb6f69876e0 | NYU-49 | 2021-07-22 | 001  | probe00 |
            """
            # print(f'| {pid} | {rec.eid} | {rec.subject} | {rec.date} | {rec.number}  | {rec.pname} |')
            # register_histology(one, pid, ProbeRegisterTaskB)
            pass

df_pids.to_parquet("/home/olivier/PycharmProjects/alyx_ferret/iblalyx/releases/2025_Q3_Noel_et_al_Autism_pids.pqt")

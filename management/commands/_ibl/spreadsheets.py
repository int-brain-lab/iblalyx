"""
Randomly assign ephys session to do histology alignment.
Assign new person to do histology alignment.
"""

from pathlib import Path
import pandas as pd
import numpy as np
import sys
from datetime import date
import os

from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import client, file, tools

from data.models import Session
from misc.models import LabMember
from experiments.models import ProbeInsertion, TrajectoryEstimate

# TODO remove printed txt to file for debugging
'''
# -- Save print into text file
path_save_txt = Path.home().joinpath('report_gc_temp')

# Filename with appended date
today = date.today()
d1 = today.strftime("%Y-%m-%d")
file_str = 'histology_assign_update'
filename = f'{d1}__{file_str}__varout.txt'

# Delete any older file
list_file_out = os.listdir(path_save_txt)
matching_file_str = [s for s in list_file_out if file_str in s]
for i_file in matching_file_str:
    file_to_del = path_save_txt.joinpath(i_file)
    os.remove(file_to_del)

# Save printed text - init
orig_stdout = sys.stdout
f = open(path_save_txt.joinpath(filename), 'w')
sys.stdout = f
'''

# -- FUNCTIONS TO TEST DATASET TYPE EXISTENCE
LIST_STR_ID = [
    'ephys_raw_probe_ds',
    'ephys_raw_qc_ds',
    'ks2_probe_ds',
    'ephys_raw_sess_ds',
    'ephys_passive_raw',
    'ephys_passive_sess_ds',
    'trials_raw_ds',
    'trials_ds',
    'wheel_ds',
    'camera_raw_ds',
    'camera_framecount',
    'dlc_ds'
]


def _select_dataset(str_identifier):
    '''
    Check if all datasets associated to insertion/session exist.
    :param: str_identifier: identifier string to know what ds to check for
    :return: ds_spec: list of ds type, ins_or_sess: str "ins" or "sess"
    '''

    if str_identifier not in LIST_STR_ID:
        raise ValueError("str_identifier value not matching any predefined possible values.")

    # A) --- DATASET AT INSERTION LEVEL

    # -- EPHYS RAW FILES PER PROBE
    if str_identifier == 'ephys_raw_probe_ds':
        ds_spec = [
            'ephysData.raw.ap',
            'ephysData.raw.ch',
            'ephysData.raw.lf',
            'ephysData.raw.meta'
        ]
        ins_or_sess = 'ins'

    # -- EPHYS EXTRACTED DS QC PER PROBE
    elif str_identifier == 'ephys_raw_qc_ds':
        ds_spec = [
            '_iblqc_ephysSpectralDensity.freqs',
            '_iblqc_ephysSpectralDensity.power',
            '_iblqc_ephysTimeRms.rms',
            '_iblqc_ephysTimeRms.timestamps'
        ]
        ins_or_sess = 'ins'

    # --- KS2 EXTRACTED DATASETS AT PROBE LEVEL
    elif str_identifier == 'ks2_probe_ds':
        ds_spec = [
            'spikes.depths',
            'spikes.clusters',
            'spikes.times',
            'clusters.depths',
            'clusters.channels',
            'clusters.amps',
            'clusters.peakToTrough',
            'clusters.waveforms',
            # 'clusters.metrics',
            'channels.localCoordinates',
            'channels.rawInd'
        ]
        ins_or_sess = 'ins'


    # B) --- DATASET AT SESSION LEVEL
    # -- EPHYS RAW FILES PER SESSION
    elif str_identifier == 'ephys_raw_sess_ds':
        ds_spec = [
            'ephysData.raw.nidq',
            'ephysData.raw.sync',
            'ephysData.raw.timestamps',
            '_spikeglx_sync.channels',
            '_spikeglx_sync.polarities',
            '_spikeglx_sync.times'
        ]
        ins_or_sess = 'sess'

    # -- PASSIVE RAW FILES
    elif str_identifier == 'ephys_passive_raw':
        ds_spec = [
            '_iblrig_RFMapStim.raw'
        ]
        ins_or_sess = 'sess'

    # -- PASSIVE EXTRACTED DS
    elif str_identifier == 'ephys_passive_sess_ds':
        ds_spec = [
            '_ibl_passivePeriods.intervalsTable',
            '_ibl_passiveRFM.times',
            '_ibl_passiveGabor.table',
            '_ibl_passiveStims.table'
            ]
        ins_or_sess = 'sess'

    # -- TRIALS RAW FILES
    elif str_identifier == 'trials_raw_ds':
        ds_spec = [
            # '_iblrig_codeFiles.raw',
             '_iblrig_encoderEvents.raw',
             '_iblrig_encoderPositions.raw',
             '_iblrig_encoderTrialInfo.raw',
             # '_iblrig_micData.raw',
             '_iblrig_stimPositionScreen.raw',
             '_iblrig_syncSquareUpdate.raw',
             '_iblrig_taskData.raw',
             '_iblrig_taskSettings.raw',
             # '_iblrig_VideoCodeFiles.raw'
            ]
        ins_or_sess = 'sess'

    # -- TRIALS EXTRACTED DS
    elif str_identifier == 'trials_ds':
        ds_spec = [
            'trials.choice',
            'trials.contrastLeft',
            'trials.contrastRight',
            'trials.feedback_times',
            'trials.feedbackType',
            # 'trials.firstMovement_times',    # should be there on new session
            'trials.goCue_times',
            #'trials.goCueTrigger_times',
            # 'trials.included',
            'trials.intervals',
            # 'trials.itiDuration',
            'trials.probabilityLeft',
            # 'trials.repNum',
            'trials.response_times',
            'trials.rewardVolume',
            #'trials.stimOff_times',   # should be there on new session
            'trials.stimOn_times'
        ]
        ins_or_sess = 'sess'

    # -- WHEEL DS
    elif str_identifier == 'wheel_ds':
        ds_spec = [
             'wheelMoves.intervals',
             'wheelMoves.peakAmplitude',
             'wheel.position',
             'wheel.timestamps']
        ins_or_sess = 'sess'

    # -- CAMERA RAW FILES
    elif str_identifier == 'camera_raw_ds':
        ds_spec = [
            '_iblrig_rightCamera.timestamps',
            '_iblrig_rightCamera.raw',
            '_iblrig_bodyCamera.timestamps',
            '_iblrig_bodyCamera.raw',
            '_iblrig_leftCamera.timestamps',
            '_iblrig_leftCamera.raw'
        ]
        ins_or_sess = 'sess'

    # -- CAMERA FRAME COUNTER
    elif str_identifier == 'camera_framecount':
        ds_spec = [
            '_iblrig_bodyCamera.frame_counter.bin',
            '_iblrig_rightCamera.frame_counter.bin',
            '_iblrig_leftCamera.frame_counter.bin',
            # '_iblrig_Camera.GPIO',
        ]
        ins_or_sess = 'sess'

    # -- DLC DS
    elif str_identifier == 'dlc_ds':
        ds_spec = [
            '_ibl_bodyCamera.dlc',
            '_ibl_bodyCamera.times',
            '_ibl_rightCamera.dlc',
            '_ibl_rightCamera.times',
            '_ibl_leftCamera.dlc',
            '_ibl_leftCamera.times'
        ]
        ins_or_sess = 'sess'

    return ds_spec, ins_or_sess


def _test_select_dataset():
    out1, out2 = _select_dataset(str_identifier='dlc_ds')
    exp_out1 = ['_ibl_bodyCamera.dlc',
                '_ibl_bodyCamera.times',
                '_ibl_rightCamera.dlc',
                '_ibl_rightCamera.times',
                '_ibl_leftCamera.dlc',
                '_ibl_leftCamera.times']
    exp_out2 = 'sess'
    assert (out1 == exp_out1)
    assert (out2 == exp_out2)


def _check_ds_exist(insertion, ds_spec, ins_or_sess):
    '''
    Check whether there is at least one DS type of each kind per ins/sess.
    :param insertion: object Insertion
    :param ds_spec: list of datasets
    :param ins_or_sess: string "ins" or "sess"
    :return: count_overall: bool indicating if all ds are found
    '''

    # Count N dataset present and assign bool output
    count_overall = True
    for dataset in ds_spec:
        if ins_or_sess == 'ins':
            count_ds = insertion.datasets.filter(dataset_type__name__icontains=dataset).count() >= 1
        elif ins_or_sess == 'sess':
            count_ds = insertion.session.data_dataset_session_related.filter(name__icontains=dataset).count() >=1

        # Combine count ds
        count_overall = count_overall and count_ds
    return count_overall


def _check_ds_exist_main(insertion, str_identifier):
    ds_spec, ins_or_sess = _select_dataset(str_identifier)
    count_overall = _check_ds_exist(insertion, ds_spec, ins_or_sess)
    return count_overall

# -- END OF FUNCTIONS


def histology_assign_update():
    """
    Randomly assign ephys session to do histology alignment.
    Assign new person to do histology alignment.
    """
    # -- FUNCTION TO TEST DATASET TYPE EXISTENCE


    users = LabMember.objects.all()
    # Define paths to authentication details
    credentials_file_path = Path.home().joinpath('.google', 'credentials.json')
    clientsecret_file_path = Path.home().joinpath('.google', 'client_secret.json')

    SCOPE = ['https://www.googleapis.com/auth/spreadsheets']
    # See if credentials exist
    store = file.Storage(credentials_file_path)
    credentials = store.get()
    # If not get new credentials
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(clientsecret_file_path, SCOPE)
        credentials = tools.run_flow(flow, store)

    # Test getting data from sheet
    drive_service = build('drive', 'v3', http=credentials.authorize(Http()))
    sheets = build('sheets', 'v4', http=credentials.authorize(Http()))
    read_spreadsheetID = '1nidCu7MjLrjaA8NHWYnJavLzQBjILZhCkYt0OdCUTxg'
    read_spreadsheetRange = 'TEST1'  # TODO'NEW_2'
    rows = sheets.spreadsheets().values().get(spreadsheetId=read_spreadsheetID,
                                              range=read_spreadsheetRange).execute()

    data_sheet = pd.DataFrame(rows.get('values'))
    data_sheet = data_sheet.rename(columns=data_sheet.iloc[0]).drop(data_sheet.index[0]).reset_index(drop=True)
    data_sheet = data_sheet[data_sheet['sess_id'] != ""]

    # those are the sessions for which the histology alignment task has not been assigned
    sessions = Session.objects.filter(
        task_protocol__icontains='_iblrig_tasks_ephysChoiceWorld',
        project__name='ibl_neuropixel_brainwide_01',
        subject__actions_sessions__procedures__name='Histology',
        json__IS_MOCK=False,
    ).exclude(
        id__in=data_sheet.sess_id)

    if sessions.count() > 0:
        pass

    # restrict to not assigned EIDs
    # sum the amount of times a given lab is found in origin/assign lab
    df_test = pd.DataFrame()
    df_test['labs'] = pd.concat([data_sheet['origin_lab'], data_sheet['assign_lab']])
    df_test['tosum'] = 1  # Pad with ones for summing
    df_sum = df_test.groupby(['labs']).sum()
    # Create dict out of pandas dataframe for easier compute
    d = df_sum.to_dict()
    d = d['tosum']
    d.pop('', None)
    # This gives a dict like:
    # {'angelakilab': 145,
    #  'churchlandlab': 174,
    #  'cortexlab': 117}


    ## creates temp dataframe
    df_tp = {'eids': [str(i[0]) for i in sessions.values_list('id')],
             'origin_labs': [i[0] for i in sessions.values_list('lab__name')],
             'assign_labs': [None for i in range(sessions.count())],
             'subjects': [i[0] for i in sessions.values_list('subject__name')],
             'dates': [str(i[0])[:10] for i in sessions.values_list('start_time')]}

    for i, ses in enumerate(sessions):
        # remove lab key (that is the origin lab)
        assert df_tp['eids'][i] == str(ses.id)  # we never know sometimes with querysets...
        d_remove = d
        d_remove.pop(ses.lab.name, None)
        # find the lab with min assignment overall and assign it
        lab_min = min(d_remove, key=d_remove.get)
        df_tp['assign_labs'][i] = lab_min
        d[lab_min] = d[lab_min] + 1  # increase sum value


    # ======================================
    # Update insertion sheet

    # Take unique eids only
    eids_ds = list(data_sheet['sess_id'])
    eids_ds_unique, indx_un = np.unique(eids_ds, return_index=True)

    origin_ds = np.array(data_sheet['origin_lab'])
    origin_ds_unique = origin_ds[indx_un]

    assign_ds = np.array(data_sheet['assign_lab'])
    assign_ds_unique = assign_ds[indx_un]

    # Remove trailing white space from ds got from sheet
    ind_whitesp = np.where(eids_ds_unique != '')
    eids_ds_clean = eids_ds_unique[ind_whitesp]
    origin_ds_clean = origin_ds_unique[ind_whitesp]
    assign_ds_clean = assign_ds_unique[ind_whitesp]

    # create general DF
    data = {'eids': np.append(df_tp['eids'], eids_ds_clean),
            'origin_labs': np.append(df_tp['origin_labs'], origin_ds_clean),
            'assign_labs': np.append(df_tp['assign_labs'], assign_ds_clean)
            }

    # ------- launch from there on Alyx to test

    list_all = []
    n_sess = len(data['eids'])
    for i_sess in range(0, n_sess):
        eid = data['eids'][i_sess]
        print(f'{i_sess + 1} / {n_sess}')
        insertions = ProbeInsertion.objects.filter(session=eid)
        for i_ins, insertion in enumerate(insertions):
            subject = insertion.session.subject.nickname
            date = str(insertion.session.start_time)[:10]
            # Insertion missing Json - flag bug
            if insertion.json is None:
                print(f'WARNING: Data inconsistency: insertion id {insertion.id} does not have json field (NoneType);'
                      f'setting aligned = False')
                aligned = False
            else:
                ext_qc = insertion.json.get('extended_qc', None)
                aligned = False if ext_qc is None else ext_qc.get('alignment_resolved', False)
            # Check if user assigned did align
            origin_lab = data['origin_labs'][i_sess]
            assign_lab = data['assign_labs'][i_sess]

            # provenance - 70: Ephys aligned histology track / 50: Histology track / 30: Micro-manipulator / 10: Planned
            traj = TrajectoryEstimate.objects.filter(provenance=70, probe_insertion=insertion.id)

            origin_lab_done = False
            assign_lab_done = False
            if traj.count() > 0:
                if traj[0].json is not None:
                    names = traj[0].json.keys()
                    names = list(names)

                    for i_name in range(0, len(names)):
                        idx_str = str.find(names[i_name], '_')
                        user_str = names[i_name][idx_str + 1:]
                        user = LabMember.objects.get(username=user_str)
                        user_lab = user.lab
                        # add hoferlab to mrsicflogel (1 lab for both)
                        if 'hoferlab' in user_lab:
                            user_lab.append('mrsicflogellab')

                        # Note: One user (e.g. chrisk) can have multiple labs, hence the "in"
                        if origin_lab in user_lab:
                            origin_lab_done = True
                        elif assign_lab in user_lab:
                            assign_lab_done = True

            # Check if insertion is critical, criteria:
            # - session critical
            # - insertion critical
            # - behavior fail
            # - impossible to trace
            # ext_qc
            is_critical = insertion.session.qc == 50  # session critical status
            if insertion.session.extended_qc is not None:
                is_critical |= insertion.session.extended_qc.get('behavior', 1) == 0  # behaviour critical
            if insertion.json is None:
                print(f'WARNING: Data inconsistency: insertion id {insertion.id} does not have json field (NoneType);'
                      f'setting is_critical = False')
                is_critical = False
            else:
                is_critical |= insertion.json.get('qc', None) == 'CRITICAL'
                is_critical |= not insertion.json.get('extended_qc', {}).get('tracing_exists', True)

            # Form dict
            dict_ins = {
                "sess_id": eid,
                "ins_id": str(insertion.id),
                "subject": subject,
                "date": date,
                "probe": insertion.name,
                "origin_lab": origin_lab,
                "origin_lab_done": origin_lab_done,
                "assign_lab": assign_lab,
                "assign_lab_done": assign_lab_done,
                "align_solved": aligned,
                "is_critical": is_critical
            }

            # Check all datasets exist and append
            for str_identifier in LIST_STR_ID:
                out_bool = _check_ds_exist_main(insertion, str_identifier)
                dict_ins[str_identifier] = out_bool

            list_all.append(dict_ins)

    # Create DF and write to sheet

    df = pd.DataFrame(list_all)
    df = df.sort_values(by=['assign_lab', 'sess_id'], ascending=True)

    # get data from sheet once again (broken pipe error otherwise)
    sheets = build('sheets', 'v4', http=credentials.authorize(Http()))
    write_spreadsheetID = read_spreadsheetID
    write_spreadsheetRange = 'NEW_2'
    write_data = sheets.spreadsheets(). \
        values().update(spreadsheetId=write_spreadsheetID, valueInputOption='RAW',  # USER_ENTERED
                        range=write_spreadsheetRange,
                        body=dict(majorDimension='ROWS',
                                  values=df.T.reset_index().T.values.tolist())).execute()
    print('Sheet successfully Updated')

    # TODO remove printed txt to file for debugging
    '''
    # Save printed text
    sys.stdout = orig_stdout
    f.close()
    '''

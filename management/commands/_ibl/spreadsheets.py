"""
Write to sheet histology assignment
"""

from pathlib import Path
import pandas as pd
from django.db.models import Q
import numpy as np
import sys
from datetime import date
import os

from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import client, file, tools

from misc.models import LabMember, Lab
from experiments.models import ProbeInsertion, TrajectoryEstimate


# -- FUNCTIONS TO TEST DATASET TYPE EXISTENCE
LIST_STR_ID = [
    # 'ephys_raw_probe_ds',
    # 'ephys_raw_qc_ds',
    'ks2_probe_ds',
    # 'ephys_raw_sess_ds',
    # 'ephys_passive_raw',
    'ephys_passive_sess_ds',
    # 'trials_raw_ds',
    # 'trials_ds',
    # 'wheel_ds',
    # 'camera_raw_ds',
    # 'camera_framecount',
    # 'dlc_ds'
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


def _populate_sheet(insertions, spreadsheetID, spreadsheetRange):

    list_all = []
    for i_ins, insertion in enumerate(insertions):
        print(f'{i_ins} / {len(insertions)}')
        n_alignment_done = 0
        names_alignment = ''
        subject = insertion.session.subject.nickname
        date = str(insertion.session.start_time)[:10]

        # Init var
        origin_lab = insertion.session.lab.name
        assign_lab = insertion.json['todo_alignment']
        origin_lab_done = False
        assign_lab_done = False

        # Check if user assigned did align
        traj = TrajectoryEstimate.objects.filter(provenance=70, probe_insertion=insertion.id)

        if traj.count() > 0:
            if traj[0].json is not None:
                names = traj[0].json.keys()
                names = list(names)

                # N alignments done
                n_alignment_done = len(names)
                names_alignment = [item[str.find(item, '_')+1:] for item in names]
                names_alignment = ', '.join(names_alignment)

                for i_name in range(0, len(names)):
                    idx_str = str.find(names[i_name], '_')
                    user_str = names[i_name][idx_str + 1:]

                    if len(LabMember.objects.filter(username=user_str)) > 0:  # Make sure username exits
                        user = LabMember.objects.get(username=user_str)
                        user_lab = user.lab
                        # append cases where persons are in multiple labs
                        if 'hoferlab' in user_lab:
                            user_lab.append('mrsicflogellab')
                        if 'churchlandlab' in user_lab:
                            user_lab.append('churchlandlab_ucla')
                        if user.username == 'chrisk':  # do count only alignment done for Zador lab
                            user_lab = ['zadorlab']

                        # Note: One user (e.g. chrisk) can have multiple labs, hence the "in"
                        if origin_lab in user_lab:
                            origin_lab_done = True
                        elif assign_lab in user_lab:
                            assign_lab_done = True

        # Form dict
        dict_ins = {
            "sess_id": str(insertion.session.id),
            "ins_id": str(insertion.id),
            "subject": subject,
            "date": date,
            "probe": insertion.name,
            "origin_lab": origin_lab,
            "origin_lab_done": origin_lab_done,
            "assign_lab": assign_lab,
            "assign_lab_done": assign_lab_done,
            "n_alignment_done": n_alignment_done,
            "names_alignment": names_alignment
        }

        # Check all datasets exist and append
        for str_identifier in LIST_STR_ID:
            out_bool = _check_ds_exist_main(insertion, str_identifier)
            dict_ins[str_identifier] = out_bool

        list_all.append(dict_ins)

    # Create DF to write to sheet
    df = pd.DataFrame(list_all)
    df = df.sort_values(by=['assign_lab', 'sess_id'], ascending=True)

    # --- CONNECT TO G-SHEET ---
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

    # Connect to G-sheet in write-access, and write in the dataframe
    sheets = build('sheets', 'v4', http=credentials.authorize(Http()))
    write_spreadsheetID = spreadsheetID
    write_spreadsheetRange = spreadsheetRange

    # Clear sheet first
    request = sheets.spreadsheets(). \
        values().clear(spreadsheetId=write_spreadsheetID, range=write_spreadsheetRange)
    response = request.execute()

    write_data = sheets.spreadsheets(). \
        values().update(spreadsheetId=write_spreadsheetID, valueInputOption='RAW',  # USER_ENTERED
                        range=write_spreadsheetRange,
                        body=dict(majorDimension='ROWS',
                                  values=df.T.reset_index().T.values.tolist())).execute()
    print('Sheet successfully Updated')


def _query_insertions():
    '''
    Find insertions that do not have alignment assigned to them
    :return:
    '''

    all_insertions1 = ProbeInsertion.objects.filter(
        session__task_protocol__icontains='_iblrig_tasks_ephysChoiceWorld',
        session__project__name='ibl_neuropixel_brainwide_01',
        session__subject__actions_sessions__procedures__name='Histology',
        session__json__IS_MOCK=False,
        session__extended_qc__behavior=1,  # Added 2022-04-04
        session__qc__lt=50  # Added 2022-04-04
    )

    all_insertions = all_insertions1.filter(~Q(json__qc='CRITICAL')) # Added 2022-04-04

    insertions = all_insertions.filter(
        json__todo_alignment__isnull=True
        )
    return insertions, all_insertions


def _histology_assign():
    '''
    Assign a lab to perform histology
    '''
    exclude_lab = ['hoferlab', 'churchlandlab']
    labs = Lab.objects.all().exclude(name__in=exclude_lab)

    insertions, all_insertions = _query_insertions()  # Init
    print(len(insertions))
    len_previous = len(all_insertions)
    while len(insertions) > 0:
        # Work on first insertion off the list
        insertion = insertions[0]

        # Find if there are other insertions from same session
        insertions_tochange = insertions.filter(session__id=insertion.session.id)

        # Compute which lab should be assigned
        for lab in labs:
            # Note:
            # churchlandlab == churchlandlab_ucla
            # mrsicflogellab == hoferlab

            if lab.name == 'churchlandlab_ucla':
                insertions_lab_done = all_insertions.filter(session__lab__name__icontains='churchlandlab')
                insertions_lab_assigned = all_insertions.filter(json__todo_alignment__icontains='churchlandlab')
                len_ins_total_lab = len(insertions_lab_done) + len(insertions_lab_assigned)

            elif lab.name == 'mrsicflogellab':
                len_lab_done = len(all_insertions.filter(session__lab__name=lab.name)) + \
                               len(all_insertions.filter(session__lab__name='hoferlab'))
                insertions_lab_assigned = all_insertions.filter(json__todo_alignment=lab.name)
                len_ins_total_lab = len_lab_done + len(insertions_lab_assigned)

            else:
                insertions_lab_done = all_insertions.filter(session__lab__name=lab.name)
                insertions_lab_assigned = all_insertions.filter(json__todo_alignment=lab.name)
                len_ins_total_lab = len(insertions_lab_done) + len(insertions_lab_assigned)

            # Find minimum and assign
            if len_ins_total_lab < len_previous:  # Should always go into this loop at first pass
                len_previous = len_ins_total_lab
                lab_assigned = lab.name

        # Change JSON field of insertions
        for pi in insertions_tochange:
            d = pi.json
            d['todo_alignment'] = lab_assigned
            pi.json = d
            pi.save()

        # See if any insertions remaining to be assigned
        insertions, all_insertions = _query_insertions()
        print(len(insertions))



def histology_assign_update():
    """
    Randomly assign ephys session to do histology alignment.
    Update G-sheet
    """
    # Assign histology to insertions that do not have it
    _histology_assign()

    # Once done:
    # Take only insertion for BW project, with histology, remove critical insertions
    all_insertions = ProbeInsertion.objects.filter(
        session__task_protocol__icontains='_iblrig_tasks_ephysChoiceWorld',
        session__project__name='ibl_neuropixel_brainwide_01',
        session__subject__actions_sessions__procedures__name='Histology',
        session__json__IS_MOCK=False,
        session__qc__lt=50,
        session__extended_qc__behavior=1,
        json__extended_qc__tracing_exists=True
    ) | ProbeInsertion.objects.filter(  # Some sessions do not have IS_MOCK as field
        session__task_protocol__icontains='_iblrig_tasks_ephysChoiceWorld',
        session__project__name='ibl_neuropixel_brainwide_01',
        session__subject__actions_sessions__procedures__name='Histology',
        session__json__IS_MOCK__isnull=True,
        session__qc__lt=50,
        session__extended_qc__behavior=1,
        json__extended_qc__tracing_exists=True
    )

    # Get list of all insertions that are not yet resolved but have >=2 alignments
    insertions_toresolve = all_insertions.filter(
        json__extended_qc__alignment_resolved=False,
        json__extended_qc__alignment_count__gte=2
    )
    # Get list of all insertions that have <2 alignments
    insertions_toalign = all_insertions.filter(
        json__extended_qc__alignment_count__lt=2,
        json__extended_qc__tracing_exists=True
    ) | all_insertions.filter(  # account for fact that field does not exist if no alignment ever done
        json__extended_qc__alignment_count__isnull=True,
        json__extended_qc__tracing_exists=True
    )
    # # Get list of all insertions that miss tracing
    # insertions_totrace = all_insertions.filter(
    #     json__extended_qc__tracing_exists=False
    # )

    print('RESOLUTION MISSING')
    _populate_sheet(insertions = insertions_toresolve,
                    spreadsheetID = '1nidCu7MjLrjaA8NHWYnJavLzQBjILZhCkYt0OdCUTxg',
                    spreadsheetRange = 'RESOLUTION_MISSING')
    print('ALIGNMENT MISSING')
    _populate_sheet(insertions = insertions_toalign,
                    spreadsheetID = '1nidCu7MjLrjaA8NHWYnJavLzQBjILZhCkYt0OdCUTxg',
                    spreadsheetRange = 'ALIGNMENT_MISSING')
    # print('TRACING MISSING')
    # TODO other function as assign lab does not exist
    # _populate_sheet(insertions = insertions_totrace,
    #                 spreadsheetID = '1nidCu7MjLrjaA8NHWYnJavLzQBjILZhCkYt0OdCUTxg',
    #                 spreadsheetRange = 'TRACING_MISSING')

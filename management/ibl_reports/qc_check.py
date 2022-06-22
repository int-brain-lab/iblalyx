FAIL_COLOUR = "#FF6384"
PASS_COLOUR = "#79AEC8"
IMPORTANT_BORDER = "#000000"
NORMAL_BORDER = "#417690"

QC_DICT = {
    '50': 'CRITICAL',
    '40': 'FAIL',
    '30': 'WARNING',
    '0' :'NOT_SET',
    '10': 'PASS',
}


def get_task_qc_colours(task_qc_data):
    colour = []
    border = []

    critical_keys = [
        '_task_stimOn_goCue_delays',
        '_task_response_feedback_delays',
        '_task_wheel_move_before_feedback',
        '_task_wheel_freeze_during_quiescence',
        '_task_error_trial_event_sequence',
        '_task_correct_trial_event_sequence',
        '_task_reward_volumes',
        '_task_reward_volume_set',
        '_task_stimulus_move_before_goCue',
        '_task_audio_pre_trial',
        '_task_n_trial_events'
    ]

    for key, val in task_qc_data.items():
        if val < 0.9:
            col = FAIL_COLOUR
        else:
            col = PASS_COLOUR

        if key in critical_keys:
            bord = IMPORTANT_BORDER
        else:
            bord = col

        colour.append(col)
        border.append(bord)

    return colour, border


def process_video_qc(video_qc_data):
    data_dict = {'data': [],
                 'label': [],
                 'colour': []}

    for key, value in video_qc_data.items():

        if isinstance(value, list):
            if isinstance(value[0], bool):
                value = value[0]
        data_dict['label'].append(key)
        if value is None:
            data_dict['data'].append(0)
            data_dict['colour'].append(PASS_COLOUR)
        elif value:
            data_dict['data'].append(1)
            data_dict['colour'].append(PASS_COLOUR)
        else:
            data_dict['data'].append(-1)
            data_dict['colour'].append(FAIL_COLOUR)

    return data_dict


def behav_summary(qc_data):
    if qc_data is None:
        return 'NOT_SET'

    behav = qc_data.get('behavior', 'NOT_SET')
    if behav == 1:
        return 'PASS'
    elif behav == 0:
        return 'FAIL'
    else:
        return 'NOT_SET'


def qc_summary(qc_data):

    if qc_data is None:
        data_dict = {}
        data_dict['Task QC'] = ['NOT_SET', 0, 0, 0]
        data_dict['Video Body QC'] = ['NOT_SET', 0, 0, 0]
        data_dict['Video Left QC'] = ['NOT_SET', 0, 0, 0]
        data_dict['Video Right QC'] = ['NOT_SET', 0, 0, 0]
        data_dict['DLC Body QC'] = ['NOT_SET', 0, 0, 0]
        data_dict['DLC Left QC'] = ['NOT_SET', 0, 0, 0]
        data_dict['DLC Right QC'] = ['NOT_SET', 0, 0, 0]

        return data_dict

    data_dict = {}
    data_dict['Task QC'] = [qc_data.get('task', 'NOT_SET'), 0, 0, 0]
    data_dict['Video Body QC'] = [qc_data.get('videoBody', 'NOT_SET'), 0, 0, 0]
    data_dict['Video Left QC'] = [qc_data.get('videoLeft', 'NOT_SET'), 0, 0, 0]
    data_dict['Video Right QC'] = [qc_data.get('videoRight', 'NOT_SET'), 0, 0, 0]
    data_dict['DLC Body QC'] = [qc_data.get('dlcBody', 'NOT_SET'), 0, 0, 0]
    data_dict['DLC Left QC'] = [qc_data.get('dlcLeft', 'NOT_SET'), 0, 0, 0]
    data_dict['DLC Right QC'] = [qc_data.get('dlcRight', 'NOT_SET'), 0, 0, 0]

    for key, value in qc_data.items():
        if '_task_' in key:
            data_dict['Task QC'][3] += 1
            if value >= 0.9:
                data_dict['Task QC'][1] += 1
            else:
                data_dict['Task QC'][2] += 1
        if '_video' in key or '_dlc' in key:

            if '_video' in key:
                name = 'Video'
            elif '_dlc' in key:
                name = 'DLC'

            if isinstance(value, list):
                if isinstance(value[0], bool):
                    value = value[0]

            if value is None:
                if 'Left' in key:
                    data_dict[f'{name} Left QC'][3] += 1
                elif 'Right' in key:
                    data_dict[f'{name} Right QC'][3] += 1
                elif 'Body' in key:
                    data_dict[f'{name} Body QC'][3] += 1
            elif value:
                if 'Left' in key:
                    data_dict[f'{name} Left QC'][3] += 1
                    data_dict[f'{name} Left QC'][1] += 1
                elif 'Right' in key:
                    data_dict[f'{name} Right QC'][1] += 1
                    data_dict[f'{name} Right QC'][3] += 1
                elif 'Body' in key:
                    data_dict[f'{name} Body QC'][1] += 1
                    data_dict[f'{name} Body QC'][3] += 1
            else:
                if 'Left' in key:
                    data_dict[f'{name} Left QC'][3] += 1
                    data_dict[f'{name} Left QC'][2] += 1
                elif 'Right' in key:
                    data_dict[f'{name} Right QC'][2] += 1
                    data_dict[f'{name} Right QC'][3] += 1
                elif 'Body' in key:
                    data_dict[f'{name} Body QC'][2] += 1
                    data_dict[f'{name} Body QC'][3] += 1

    for key, value in data_dict.items():
        if value[1] == value[2] == 0:
            data_dict[key][1] = None
            data_dict[key][2] = None
            data_dict[key][3] = None

    return data_dict








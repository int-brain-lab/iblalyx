FAIL_COLOUR = "#FF6384"
PASS_COLOUR = "#79AEC8"
IMPORTANT_BORDER = "#000000"
NORMAL_BORDER = "#417690"

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
        if key == '_videoBody_wheel_alignment':
            continue

        if isinstance(value, list):
            if isinstance(value[0], bool):
                data_dict['label'].append(key)
                if value[0]:
                    data_dict['data'].append(1)
                    data_dict['colour'].append(PASS_COLOUR)
                else:
                    data_dict['data'].append(-1)
                    data_dict['colour'].append(FAIL_COLOUR)
        else:
            data_dict['label'].append(key)
            if value:
                data_dict['data'].append(1)
                data_dict['colour'].append(PASS_COLOUR)
            else:
                data_dict['data'].append(-1)
                data_dict['colour'].append(FAIL_COLOUR)


    return data_dict


def behav_summary(behav):
    if behav == 1:
        return 'PASS'
    elif behav == 0:
        return 'FAIL'
    else:
        return 'NOT_SET'

def qc_summary(qc_data):

    data_dict = {}
    data_dict['Task QC'] = [qc_data.get('task', 'NOT_SET'), 0, 0]
    data_dict['Video Body QC'] = [qc_data.get('videoBody', 'NOT_SET'), 0, 0]
    data_dict['Video Left QC'] = [qc_data.get('videoLeft', 'NOT_SET'), 0, 0]
    data_dict['Video Right QC'] = [qc_data.get('videoRight', 'NOT_SET'), 0, 0]
    data_dict['DLC Body QC'] = [qc_data.get('dlcBody', 'NOT_SET'), 0, 0]
    data_dict['DLC Left QC'] = [qc_data.get('dlcLeft', 'NOT_SET'), 0, 0]
    data_dict['DLC Right QC'] = [qc_data.get('dlcRight', 'NOT_SET'), 0, 0]

    for key, value in qc_data.items():
        if '_task_' in key:
            if value >= 0.9:
                data_dict['Task QC'][1] += 1
            else:
                data_dict['Task QC'][2] += 1
        if '_video' in key:
            if isinstance(value, list):
                if isinstance(value[0], bool):
                    if value[0]:
                        if 'Left' in key:
                            data_dict['Video Left QC'][1] += 1
                        elif 'Right' in key:
                            data_dict['Video Right QC'][1] += 1
                        elif 'Body' in key:
                            data_dict['Video Body QC'][1] += 1
                    else:
                        if 'Left' in key:
                            data_dict['Video Left QC'][2] += 1
                        elif 'Right' in key:
                            data_dict['Video Right QC'][2] += 1
                        elif 'Body' in key:
                            data_dict['Video Body QC'][2] += 1
            else:
                if value:
                    if 'Left' in key:
                        data_dict['Video Left QC'][1] += 1
                    elif 'Right' in key:
                        data_dict['Video Right QC'][1] += 1
                    elif 'Body' in key:
                        data_dict['Video Body QC'][1] += 1
                else:
                    if 'Left' in key:
                        data_dict['Video Left QC'][2] += 1
                    elif 'Right' in key:
                        data_dict['Video Right QC'][2] += 1
                    elif 'Body' in key:
                        data_dict['Video Body QC'][2] += 1
        if '_dlc' in key:
            if isinstance(value, list):
                if isinstance(value[0], bool):
                    if value[0]:
                        if 'Left' in key:
                            data_dict['DLC Left QC'][1] += 1
                        elif 'Right' in key:
                            data_dict['DLC Right QC'][1] += 1
                        elif 'Body' in key:
                            data_dict['DLC Body QC'][1] += 1
                    else:
                        if 'Left' in key:
                            data_dict['DLC Left QC'][2] += 1
                        elif 'Right' in key:
                            data_dict['DLC Right QC'][2] += 1
                        elif 'Body' in key:
                            data_dict['DLC Body QC'][2] += 1
            else:
                if value:
                    if 'Left' in key:
                        data_dict['DLC Left QC'][1] += 1
                    elif 'Right' in key:
                        data_dict['DLC Right QC'][1] += 1
                    elif 'Body' in key:
                        data_dict['DLC Body QC'][1] += 1
                else:
                    if 'Left' in key:
                        data_dict['DLC Left QC'][2] += 1
                    elif 'Right' in key:
                        data_dict['DLC Right QC'][2] += 1
                    elif 'Body' in key:
                        data_dict['DLC Body QC'][2] += 1

    for key, value in data_dict.items():
        if value[1] == value[2] == 0:
            data_dict[key][1] = None
            data_dict[key][2] = None

    return data_dict








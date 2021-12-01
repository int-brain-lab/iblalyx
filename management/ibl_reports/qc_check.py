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








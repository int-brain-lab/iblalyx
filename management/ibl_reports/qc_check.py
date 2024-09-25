FAIL_COLOUR = "#FF6384"
PASS_COLOUR = "#4BAD6C"
WARNING_COLOUR = "#79AEC8"
NOT_SET_COLOUR = "#B1B3C5"
IMPORTANT_BORDER = "#000000"
NORMAL_BORDER = "#417690"

COLOURS = {
    'PASS': "#4BAD6C",
    'WARNING': "#79AEC8",
    'FAIL': "#FF6384",
    'NOT_SET': "#B1B3C5",
    'CRITICAL': "#FF6384",
}

OUTCOMES = {
    'PASS': 1,
    'WARNING': 0.5,
    'FAIL': -1,
    'NOT_SET': 0,
    'CRITICAL': -1
}

QC_DICT = {
    '50': 'CRITICAL',
    '40': 'FAIL',
    '30': 'WARNING',
    '0': 'NOT_SET',
    '10': 'PASS',
}

task_criteria = dict()
task_criteria['default'] = {"PASS": 0.99, "WARNING": 0.90, "FAIL": 0}  # Note: WARNING was 0.95 prior to Aug 2022
task_criteria['_task_stimOff_itiIn_delays'] = {"PASS": 0.99, "WARNING": 0}
task_criteria['_task_positive_feedback_stimOff_delays'] = {"PASS": 0.99, "WARNING": 0}
task_criteria['_task_negative_feedback_stimOff_delays'] = {"PASS": 0.99, "WARNING": 0}
task_criteria['_task_wheel_move_during_closed_loop'] = {"PASS": 0.99, "WARNING": 0}
task_criteria['_task_response_stimFreeze_delays'] = {"PASS": 0.99, "WARNING": 0}
task_criteria['_task_detected_wheel_moves'] = {"PASS": 0.99, "WARNING": 0}
task_criteria['_task_trial_length'] = {"PASS": 0.99, "WARNING": 0}
task_criteria['_task_goCue_delays'] = {"PASS": 0.99, "WARNING": 0}
task_criteria['_task_errorCue_delays'] = {"PASS": 0.99, "WARNING": 0}
task_criteria['_task_stimOn_delays'] = {"PASS": 0.99, "WARNING": 0}
task_criteria['_task_stimOff_delays'] = {"PASS": 0.99, "WARNING": 0}
task_criteria['_task_stimFreeze_delays'] = {"PASS": 0.99, "WARNING": 0}
task_criteria['_task_iti_delays'] = {"NOT_SET": 0}
task_criteria['_task_passed_trial_checks'] = {"NOT_SET": 0}


def threshold(thresholds, qc_value):
    if qc_value is None:
        return 'NOT_SET'
    if 'PASS' in thresholds.keys() and qc_value >= thresholds['PASS']:
        return 'PASS'
    if 'WARNING' in thresholds.keys() and qc_value >= thresholds['WARNING']:
        return 'WARNING'
    if 'FAIL' in thresholds and qc_value >= thresholds['FAIL']:
        return 'FAIL'
    if 'NOT_SET' in thresholds and qc_value >= thresholds['NOT_SET']:
        return 'NOT_SET'
    # if None of this applies, return 'NOT_SET'
    return 'NOT_SET'


def get_task_qc_colours(task_qc_data):
    colour = []
    border = []
    thresholds = []
    outcomes = []
    labels = []
    vals = []

    for key, val in task_qc_data.items():
        if key in task_criteria.keys():
            continue

        crit = task_criteria['default']
        default = True

        qc_status = threshold(crit, val)
        col = COLOURS[qc_status]

        if default:
            bord = IMPORTANT_BORDER
        else:
            bord = col

        colour.append(col)
        border.append(bord)
        thresholds.append(crit)
        outcomes.append(qc_status)
        labels.append(key)
        vals.append(val)

    for key, val in task_qc_data.items():
        if key not in task_criteria.keys():
            continue

        crit = task_criteria[key]
        default = False

        qc_status = threshold(crit, val)
        col = COLOURS[qc_status]

        if default:
            bord = IMPORTANT_BORDER
        else:
            bord = col

        colour.append(col)
        border.append(bord)
        thresholds.append(crit)
        outcomes.append(qc_status)
        labels.append(key)
        vals.append(val)

    return colour, border, thresholds, outcomes, labels, vals


def determine_qc(value):

    if isinstance(value, list):
        value = value[0]

    if isinstance(value, bool):
        if value:
            qc = 'PASS'
        else:
            qc = 'FAIL'
    elif isinstance(value, str):
        qc = value
    else:
        qc = 'NOT_SET'

    return qc


def process_video_qc(video_qc_data):
    data_dict = {'data': [],
                 'label': [],
                 'colour': []}
    outcomes = []

    for key, value in video_qc_data.items():
        data_dict['label'].append(key)
        qc = determine_qc(value)
        data_dict['data'].append(OUTCOMES[qc])
        data_dict['colour'].append(COLOURS[qc])
        outcomes.append(qc)

    return data_dict, outcomes


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
    task_key = next((k for k, _ in qc_data.items() if k.startswith('task')), 'task')
    data_dict['Task QC'] = [qc_data.get(task_key, 'NOT_SET'), 0, 0, 0]
    data_dict['Video Body QC'] = [qc_data.get('videoBody', 'NOT_SET'), 0, 0, 0]
    data_dict['Video Left QC'] = [qc_data.get('videoLeft', 'NOT_SET'), 0, 0, 0]
    data_dict['Video Right QC'] = [qc_data.get('videoRight', 'NOT_SET'), 0, 0, 0]
    data_dict['DLC Body QC'] = [qc_data.get('dlcBody', 'NOT_SET'), 0, 0, 0]
    data_dict['DLC Left QC'] = [qc_data.get('dlcLeft', 'NOT_SET'), 0, 0, 0]
    data_dict['DLC Right QC'] = [qc_data.get('dlcRight', 'NOT_SET'), 0, 0, 0]

    for key, value in qc_data.items():
        if '_task_' in key:
            data_dict['Task QC'][3] += 1

            if key in task_criteria.keys():
                crit = task_criteria[key]
            else:
                crit = task_criteria['default']

            qc_status = threshold(crit, value)
            if qc_status != 'FAIL':
                data_dict['Task QC'][1] += 1
            else:
                data_dict['Task QC'][2] += 1

        if '_video' in key or '_dlc' in key:

            if '_video' in key:
                name = 'Video'
            elif '_dlc' in key:
                name = 'DLC'

            qc = determine_qc(value)

            if qc in ['PASS', 'WARNING']:
                if 'Left' in key:
                    data_dict[f'{name} Left QC'][3] += 1
                    data_dict[f'{name} Left QC'][1] += 1
                elif 'Right' in key:
                    data_dict[f'{name} Right QC'][1] += 1
                    data_dict[f'{name} Right QC'][3] += 1
                elif 'Body' in key:
                    data_dict[f'{name} Body QC'][1] += 1
                    data_dict[f'{name} Body QC'][3] += 1
            elif qc in ['FAIL', 'CRITICAL']:
                if 'Left' in key:
                    data_dict[f'{name} Left QC'][3] += 1
                    data_dict[f'{name} Left QC'][2] += 1
                elif 'Right' in key:
                    data_dict[f'{name} Right QC'][2] += 1
                    data_dict[f'{name} Right QC'][3] += 1
                elif 'Body' in key:
                    data_dict[f'{name} Body QC'][2] += 1
                    data_dict[f'{name} Body QC'][3] += 1
            else:
                if 'Left' in key:
                    data_dict[f'{name} Left QC'][3] += 1
                elif 'Right' in key:
                    data_dict[f'{name} Right QC'][3] += 1
                elif 'Body' in key:
                    data_dict[f'{name} Body QC'][3] += 1

    for key, value in data_dict.items():
        if value[1] == value[2] == 0:
            data_dict[key][1] = None
            data_dict[key][2] = None
            data_dict[key][3] = None

    return data_dict








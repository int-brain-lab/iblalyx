import ibl_reports.data_info as expected_data
from copy import deepcopy


def get_data_status(dsets, exp_dsets, title):
    datasets = []
    n_dsets = 0
    n_exp_dsets = 0
    missing = False

    for dset in exp_dsets:
        if len(dset) == 4:
            for extra in dset[3]:
                dset_data = {}
                d = dsets.filter(dataset_type__name=dset[0], collection=dset[1],
                                 name__icontains=extra)
                dset_data['type'] = dset[0] + ' - ' + extra.upper()
                if d.count() > 0:
                    d = d.first()
                    dset_data['collection'] = d.collection
                    dset_data['name'] = d.name
                    dset_data['status'] = True
                    n_dsets += 1
                else:
                    if dset[2]:
                        missing = True
                    dset_data['name'] = '-'
                    dset_data['collection'] = dset[1]
                    dset_data['status'] = False
                n_exp_dsets += 1
                datasets.append(dset_data)

        else:
            dset_data = {}
            d = dsets.filter(dataset_type__name=dset[0], collection=dset[1])
            dset_data['type'] = dset[0]
            if d.count() > 0:
                d = d.first()
                dset_data['name'] = d.name
                dset_data['collection'] = d.collection
                dset_data['status'] = True
                n_dsets += 1
            else:
                if dset[2]:
                    missing = True
                dset_data['name'] = '-'
                dset_data['collection'] = dset[1]
                dset_data['status'] = False
            n_exp_dsets += 1
            datasets.append(dset_data)


    data = {'dsets': datasets,
            'n_dsets': n_dsets,
            'n_exp_dsets': n_exp_dsets,
            'critical': missing,
            'title': title}


    return data


def get_tasks(expected_tasks, session):
    task_status = {}
    for task in expected_tasks:
        t = session.tasks.filter(name__icontains=task)

        if t.count() > 0:
            task_status[task] = t.first()
        else:
            task_status[task] = None

    return task_status


def get_probe_type(datasets, probe):

    probe_model = probe.model

    # case where the probe type hasn't been registered
    if not probe_model:
        # in this case try and figure out from data structure, 3A probes have no data in
        # raw_ephys_data collection
        dsets = datasets.filter(collection='raw_ephys_data').count()
        if dsets == 0:
            probe_type = 'Neuropixel 3A'
        else:
            probe_type = 'Neuropixel 3B2'
    else:
        probe_type = probe_model.name

    return probe_type


def raw_passive_data_status(datasets, session):

    data = get_data_status(datasets, expected_data.RAW_PASSIVE, 'Raw passive data')
    data['tasks'] = get_tasks(expected_data.RAW_PASSIVE_TASKS, session)

    return data


def passive_data_status(datasets, session):

    data = get_data_status(datasets, expected_data.PASSIVE, 'Passive data')
    data['tasks'] = get_tasks(expected_data.PASSIVE_TASKS, session)

    return data


def raw_behaviour_data_status(datasets, session):

    data = get_data_status(datasets, expected_data.RAW_BEHAVIOUR, 'Raw behaviour data')
    data['tasks'] = get_tasks(expected_data.RAW_BEHAVIOUR_TASKS, session)

    return data


def trial_data_status(datasets, session):

    data = get_data_status(datasets, expected_data.TRIALS, 'Trial data')
    data['tasks'] = get_tasks(expected_data.TRIAL_TASKS, session)

    return data


def wheel_data_status(datasets, session):

    data = get_data_status(datasets, expected_data.WHEEL, 'Wheel data')
    data['tasks'] = get_tasks(expected_data.WHEEL_TASKS, session)

    return data


def raw_ephys_data_status(datasets, session, probes):
    expected_both_dsets = []
    for ip, probe in enumerate(probes):
        probe_type = get_probe_type(datasets, probe)

        if probe_type == 'Neuropixel 3B2':
            expected_dsets = deepcopy(expected_data.RAW_EPHYS) + deepcopy(expected_data.RAW_EPHYS_EXTRA)
            if ip == 0:
                expected_dsets += deepcopy(expected_data.RAW_EPHYS_NIDAQ)
        elif probe_type == 'Neuropixel 3A':
            if '00' in probe.name:
                expected_dsets = deepcopy(expected_data.RAW_EPHYS) + deepcopy(expected_data.RAW_EPHYS_EXTRA)
            else:
                expected_dsets = deepcopy(expected_data.RAW_EPHYS)

        for dset in expected_dsets:
            if len(dset[1].split('/')) == 2:
                dset[1] = f'raw_ephys_data/{probe.name}'

        expected_both_dsets += expected_dsets

    data = get_data_status(datasets, expected_both_dsets, 'Raw ephys data')
    data['tasks'] = get_tasks(expected_data.RAW_EPHYS_TASKS, session)

    return data


def ephys_data_status(datasets, session, probes):
    expected_both_dsets = []
    for ip, probe in enumerate(probes):
        probe_type = get_probe_type(datasets, probe)

        if probe_type == 'Neuropixel 3B2' and ip == 0:
            expected_dsets = deepcopy(expected_data.EPHYS) + deepcopy(expected_data.EPHYS_NIDAQ)
        else:
            expected_dsets = deepcopy(expected_data.EPHYS)

        for dset in expected_dsets:
            if len(dset[1].split('/')) == 2:
                dset[1] = f'raw_ephys_data/{probe.name}'

        expected_both_dsets += expected_dsets

    data = get_data_status(datasets, expected_both_dsets, 'Ephys data')
    data['tasks'] = get_tasks(expected_data.EPHYS_TASKS, session)

    return data


def dlc_data_status(datasets, session):

    data = get_data_status(datasets, expected_data.DLC, 'DLC data')
    data['tasks'] = get_tasks(expected_data.DLC_TASKS, session)
    return data


def raw_video_data_status(datasets, session):

    if datasets.filter(name__icontains='Camera.frameData.bin').count() > 0:
        expected_dsets = expected_data.RAW_VIDEO + expected_data.RAW_VIDEO_NEW
    else:
        expected_dsets = expected_data.RAW_VIDEO + expected_data.RAW_VIDEO_OLD

    data = get_data_status(datasets, expected_dsets, 'Raw video data')
    data['tasks'] = get_tasks(expected_data.RAW_VIDEO_TASKS, session)

    return data

def video_data_status(datasets, probe):

    data = get_data_status(datasets, expected_data.VIDEO, 'Video data')
    data['tasks'] = get_tasks(expected_data.VIDEO_TASKS, probe)

    return data


def spikesort_data_status(datasets, session, probes):
    expected_both_dsets = []
    for probe in probes:
        expected_dsets = deepcopy(expected_data.SPIKE_SORTING)
        if datasets.filter(collection__icontains=f'alf/{probe.name}/pykilosort').count() > 0:
            for dset in expected_dsets:
                dset[1] = f'alf/{probe.name}/pykilosort'
        else:
            for dset in expected_dsets:
                dset[1] = f'alf/{probe.name}'

        expected_both_dsets += expected_dsets

    data = get_data_status(datasets, expected_both_dsets, 'Spikesorted data')

    data['tasks'] = get_tasks(expected_data.SPIKE_SORTING_TASKS, session)
    return data


def get_data_status_qs(probe_insertions):

    data_status = {'behav': [],
                   'spikesort': [],
                   'passive': [],
                   'video': []}

    critical_behav = [dset[0] for dset in expected_data.TRIALS if dset[2]]
    critical_spikesort = [dset[0] for dset in expected_data.SPIKE_SORTING if dset[2]]
    critical_passive = [dset[0] for dset in (expected_data.PASSIVE + expected_data.RAW_PASSIVE) if dset[2]]
    critical_video = [dset[0] for dset in (expected_data.VIDEO + expected_data.RAW_VIDEO) if dset[2]]

    for pr in probe_insertions:
        pr_dsets = pr.session.data_dataset_session_related
        dsets = pr_dsets.filter(collection__in=['alf'],
                                dataset_type__name__in=critical_behav)
        data_status['behav'].append(dsets.count() == len(critical_behav))

        dsets = pr_dsets.filter(collection__icontains=f'alf/{pr.name}',
                                dataset_type__name__in=critical_spikesort)
        data_status['spikesort'].append(dsets.count() >= len(critical_spikesort))

        dsets = pr_dsets.filter(collection__in=['alf', 'raw_passive_data'],
                                dataset_type__name__in=critical_passive)
        data_status['passive'].append(dsets.count() == len(critical_passive))

        dsets = pr_dsets.filter(collection__in=['alf', 'raw_video_data'],
                                dataset_type__name__in=critical_video)
        data_status['video'].append(dsets.count() == len(critical_video) * 3)

    return data_status


def get_spikesorting_task(probe_insertions):
    data_status = []
    for pr in probe_insertions:
        t = pr.session.tasks.filter(name='SpikeSorting')
        if t.count() > 0:
            task = t.first()
        else:
            task = None

        data_status.append(task)

    return data_status


def get_plots(notes, plot_type):
    plots = expected_data.PLOT_MAP[plot_type]

    ordered_plots = {}
    for pl in plots:
        note = notes.filter(text__icontains=pl)
        if note.count() > 1:
            note = note.order_by('text')
            for n in note:
                ordered_plots[n.text] = n
        elif note.count() == 1:
            ordered_plots[pl] = note.first()
        else:
            ordered_plots[pl] = None

    return ordered_plots

import pandas as pd
from data.models import Tag, Dataset
from actions.models import Session
from experiments.models import ProbeInsertion
from django.db.models import Q


# All considered probes
probes = ProbeInsertion.objects.filter(
    ~Q(json__qc='CRITICAL'),
    json__extended_qc__tracing_exists__isnull=False,
    json__extended_qc__alignment_resolved=True,
    session__project__name__icontains='ibl_neuropixel_brainwide_01',
    session__json__IS_MOCK=False,
    session__qc__lt=50,
    session__extended_qc__behavior=1,
)

probes_task = probes.filter(~Q(session__extended_qc___task_stimOn_goCue_delays__lt=0.9),
                            ~Q(session__extended_qc___task_response_feedback_delays__lt=0.9),
                            ~Q(session__extended_qc___task_wheel_move_before_feedback__lt=0.9),
                            ~Q(session__extended_qc___task_wheel_freeze_during_quiescence__lt=0.9),
                            ~Q(session__extended_qc___task_error_trial_event_sequence__lt=0.9),
                            ~Q(session__extended_qc___task_correct_trial_event_sequence__lt=0.9),
                            ~Q(session__extended_qc___task_reward_volumes__lt=0.9),
                            ~Q(session__extended_qc___task_reward_volume_set__lt=0.9),
                            ~Q(session__extended_qc___task_stimulus_move_before_goCue__lt=0.9),
                            ~Q(session__extended_qc___task_audio_pre_trial__lt=0.9))
probes_task_manual = probes.filter(session__extended_qc___experimenter_task='PASS')
probes = probes_task | probes_task_manual

# Remove probes and sessions from first release
tag = Tag.objects.get(name__icontains='2022_Q4_IBL_et_al_BWM')
bwm_1_probes = [uuid for uuid in tag.datasets.values_list('probe_insertion', flat=True).distinct() if uuid]
probes = probes.exclude(id__in=bwm_1_probes)
# This probe fulfilled all criteria, but the spike sorting couldn't be finished in time
probes = probes.exclude(id='316a733a-5358-4d1d-9f7f-179ba3c90adf')
# List of sessions is not just the sessions of those probes, for some a session might have been released in round 1
# But not both probes
sessions = Session.objects.filter(
    id__in=probes.values_list('session_id', flat=True)).exclude(
    id__in=tag.datasets.values_list('session_id', flat=True))

# Video data (some excluded)
vid_dtypes = ['camera.times', 'camera.dlc', 'camera.features', 'ROIMotionEnergy.position', 'camera.ROIMotionEnergy']
video_dsets = Dataset.objects.none()
for sess in sessions:
    for cam in ['left', 'right', 'body']:
        if sess.extended_qc[f'video{cam.capitalize()}'] != 'CRITICAL':
            video_dsets = video_dsets | Dataset.objects.filter(session=sess,
                                                               name__icontains=cam,
                                                               dataset_type__name__in=vid_dtypes,
                                                               default_dataset=True)
            video_dsets = video_dsets | Dataset.objects.filter(session=sess, default_dataset=True,
                                                               name=f'_iblrig_{cam}Camera.raw.mp4')
# Licks times should be released if either left or right cam or both released
lick_sess = Session.objects.filter(id__in=video_dsets.values_list('session_id', flat=True))
for l in lick_sess:
    if not (l.extended_qc[f'videoLeft'] == 'CRITICAL' and l.extended_qc[f'videoRight'] == 'CRITICAL'):
        video_dsets = video_dsets | Dataset.objects.filter(session=l, default_dataset=True,
                                                           dataset_type__name=f'licks.times')

# Trials data
trials_dtypes = ['trials.goCueTrigger_times', 'trials.stimOff_times', 'trials.table']
trials_dsets = Dataset.objects.filter(session__in=sessions, dataset_type__name__in=trials_dtypes, default_dataset=True)

# Wheel data
wheel_dtypes = ['wheelMoves.intervals', 'wheelMoves.peakAmplitude', 'wheel.position', 'wheel.timestamps']
wheel_dsets = Dataset.objects.filter(session__in=sessions, dataset_type__name__in=wheel_dtypes, default_dataset=True)

# Probe description dataset (session level)
probe_descr_dsets = Dataset.objects.filter(session__in=sessions, collection='alf', default_dataset=True,
                                           dataset_type__name='probes.description')

# This is session level data, if another probe of this session has already been released, we don't add it
session_ephys_dsets = Dataset.objects.filter(session__in=sessions, collection='raw_ephys_data', default_dataset=True)

# Probe related data
all_probes_dset_ids = []
for i, probe in enumerate(probes):
    collection = f'alf/{probe.name}'
    include = ['electrodeSites.brainLocationIds_ccf_2017.npy', 'electrodeSites.localCoordinates.npy',
               'electrodeSites.mlapdv.npy']
    probe_dsets = Dataset.objects.filter(session=probe.session, collection=collection, default_dataset=True, name__in=include)

    collection = f'alf/{probe.name}/pykilosort'
    probe_dsets = probe_dsets | Dataset.objects.filter(session=probe.session, collection=collection, default_dataset=True)

    collection = f'raw_ephys_data/{probe.name}'
    probe_dsets = probe_dsets | Dataset.objects.filter(session=probe.session, collection=collection,
                                                       default_dataset=True).exclude(name__icontains='ephysTimeRmsAP')

    probe_dset_ids = probe_dsets.values_list('id', flat=True)
    all_probes_dset_ids.extend(probe_dset_ids)

all_probes_dsets = Dataset.objects.filter(id__in=all_probes_dset_ids)

# Timestamp patches
ts_patch = Dataset.objects.filter(revision__name='2023-04-20')

# Combine all datasets and tag
dsets = video_dsets | trials_dsets | wheel_dsets | probe_descr_dsets | session_ephys_dsets | all_probes_dsets | ts_patch
dsets = dsets.distinct()

tag, _ = Tag.objects.get_or_create(name="2023_Q4_IBL_et_al_BWM_2", protected=True, public=True)
tag.datasets.set(dsets)

# Create session and probe dataframe
df = pd.DataFrame(
    columns=['eid', 'pid', 'probe_name'],
    data=zip([str(p.session.id) for p in probes],
             [str(p.id) for p in probes],
             [str(p.name) for p in probes])
)
df.to_csv('./2023_Q4_IBL_et_al_BWM_2_eids_pids.csv')

# Save dataset IDs
dset_ids = [str(d.id) for d in dsets]
df = pd.DataFrame(dset_ids, columns=['dataset_id'])
df.to_parquet('./2023_Q4_IBL_et_al_BWM_2_datasets.pqt')

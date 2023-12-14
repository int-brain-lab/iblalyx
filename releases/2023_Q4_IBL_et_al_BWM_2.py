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
# List of sessions is not just the sessions of those probes, for some a session might have been released in round 1
# But not both probes
sessions = Session.objects.filter(
    id__in=probes.values_list('session_id', flat=True)).exclude(
    id__in=tag.datasets.values_list('session_id', flat=True))

# Trials data
trials_dtypes = ['trials.goCueTrigger_times', 'trials.stimOff_times', 'trials.table']
trials_dsets = Dataset.objects.filter(session__in=sessions, dataset_type__name__in=trials_dtypes, default_dataset=True)

# Wheel data
wheel_dtypes = ['wheelMoves.intervals', 'wheelMoves.peakAmplitude', 'wheel.position', 'wheel.timestamps']
wheel_dsets = Dataset.objects.filter(session__in=sessions, dataset_type__name__in=wheel_dtypes, default_dataset=True)


# Video data (some excluded)
vid_dtypes = ['camera.times', 'camera.dlc', 'camera.features', 'licks.times',
              'ROIMotionEnergy.position', 'camera.ROIMotionEnergy']
alf_video_dsets = Dataset.objects.filter(session__in=sessions, dataset_type__name__in=vid_dtypes, default_dataset=True)
raw_video_dsets = Dataset.objects.filter(session__in=sessions, default_dataset=True,
                                         name__in=['_iblrig_leftCamera.raw.mp4',
                                                   '_iblrig_rightCamera.raw.mp4',
                                                   '_iblrig_bodyCamera.raw.mp4'])


# Probe description dataset (session level)
probe_descr_dsets = Dataset.objects.filter(session__in=sessions, collection='alf', default_dataset=True,
                                           dataset_type__name='probes.description')

# Probe related data
all_probes_dset_ids = []
for i, probe in enumerate(probes):
    collection = f'alf/{probe.name}'
    include = ['electrodeSites.brainLocationIds_ccf_2017.npy', 'electrodeSites.localCoordinates.npy',
               'electrodeSites.mlapdv.npy']
    probe_dsets = Dataset.objects.filter(session=probe.session, collection=collection, default_dataset=True, name__in=include)

    collection = f'alf/{probe.name}/pykilosort'
    probe_dsets = probe_dsets | Dataset.objects.filter(session=probe.session, collection=collection, default_dataset=True)

    collection = f'raw_ephys_data'
    probe_dsets = probe_dsets | Dataset.objects.filter(session=probe.session, collection=collection, default_dataset=True)

    collection = f'raw_ephys_data/{probe.name}'
    probe_dsets = probe_dsets | Dataset.objects.filter(session=probe.session, collection=collection,
                                                       default_dataset=True).exclude(name__icontains='ephysTimeRmsAP')

    probe_dset_ids = probe_dsets.values_list('id', flat=True)
    all_probes_dset_ids.extend(probe_dset_ids)

all_probes_dsets = Dataset.objects.filter(id__in=all_probes_dset_ids)

dsets = trials_dsets | wheel_dsets | alf_video_dsets | raw_video_dsets | probe_descr_dsets | all_probes_dsets
dsets = dsets.distinct()

from actions.models import Session
from django.db.models import Count, Q
import pandas as pd
import numpy as np
import time


task_keys = ['_task_iti_delays',
             '_task_goCue_delays',
             '_task_trial_length',
             '_task_stimOn_delays',
             '_task_n_trial_events',
             '_task_reward_volumes',
             '_task_stimOff_delays',
             '_task_audio_pre_trial',
             '_task_errorCue_delays',
             '_task_wheel_integrity',
             '_task_reward_volume_set',
             '_task_stimFreeze_delays',
             '_task_passed_trial_checks',
             '_task_stimOn_goCue_delays',
             '_task_detected_wheel_moves',
             '_task_stimOff_itiIn_delays',
             '_task_response_feedback_delays',
             '_task_error_trial_event_sequence',
             '_task_response_stimFreeze_delays',
             '_task_stimulus_move_before_goCue',
             '_task_wheel_move_before_feedback',
             '_task_correct_trial_event_sequence',
             '_task_wheel_move_during_closed_loop',
             '_task_wheel_freeze_during_quiescence',
             '_task_negative_feedback_stimOff_delays',
             '_task_positive_feedback_stimOff_delays',
             '_task_wheel_move_during_closed_loop_bpod']


first_pass = Count('project', filter=Q(project__name__icontains='ibl_neuropixel_brainwide_01'))
ephys_session = Session.objects.annotate(first_pass=first_pass)
ephys_session = ephys_session.filter(task_protocol__icontains='ephys').order_by('start_time')


start = time.time()
task_qc_summary = pd.DataFrame()
task_qc_summary['session'] = np.array(ephys_session.values_list('id', flat=True)).astype('str')
task_qc_summary['lab'] = np.array(ephys_session.values_list('lab__name', flat=True))
task_qc_summary['date'] = np.array(ephys_session.values_list('start_time__date', flat=True))
task_qc_summary['subject'] = np.array(ephys_session.values_list('subject__nickname', flat=True))
task_qc_summary['n_trials'] = np.array(ephys_session.values_list('n_trials', flat=True))
task_qc_summary['first_pass'] = np.array(ephys_session.values_list('first_pass', flat=True))
task_qc_summary['mock'] = np.array(ephys_session.values_list('json__IS_MOCK', flat=True))
task_qc_summary['experimenter'] = np.array(ephys_session.values_list('extended_qc__experimenter',
                                           flat=True))
task_qc_summary['task'] = np.array(ephys_session.values_list('extended_qc__task', flat=True))

for key in task_keys:
    task_qc_summary[key] = np.array(ephys_session.values_list(f'extended_qc__{key}', flat=True))

end = time.time()
print(end-start)

save_path = "/home/ubuntu/qc_table.pqt"
task_qc_summary.to_parquet(save_path)

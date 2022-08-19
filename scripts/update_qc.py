from actions.models import Session
from ibllib.qc.task_metrics import TaskQC
import pandas as pd
import time
from pathlib import Path

'''
WARNING: Don't run this on mbox (has killed the instance in the past), but connect your local alyx to the
production RDS, run this, disconnect from the RDS (!)
'''

log_path = Path.home().joinpath('qc_update')
log_path.mkdir(exist_ok=True, parents=True)

# Reaggreate task QC
# Initiate a TaskQC class, we don't need an actual session as we just need this to use some class functions
dry = True
taskQC = TaskQC('e2b845a1-e313-4a08-bc61-a5f662ed295e')
# Loop over sessions that have taskQC
sess = Session.objects.filter(extended_qc__has_key='task')
updated = []
no_task_checks = []
for s in sess:
    # Never chance critical status
    if s.extended_qc['task'] == 'CRITICAL':
        pass
    else:
        # Get all _task checks
        results = {k: v for k, v in s.extended_qc.items() if k.startswith('_task')}
        # Some sessions have only the overall task QC and none of the checks for some reason, these need to be rerun
        if len(results) == 0:
            no_task_checks.append(str(s.id))
            pass
        else:
            # Recompute the outcome from the checks
            outcome, _ = taskQC.compute_session_status_from_dict(results)
            # If outcome is different from before, update
            if outcome != s.extended_qc['task']:
                updated.append({'session': str(s.id), 'pre': s.extended_qc['task'], 'post': outcome})
                if not dry:
                    # Prevent from hitting the database to fast in a row
                    time.sleep(0.1)
                    print(str(s.id))
                    s.extended_qc['task'] = outcome
                    s.save()
df = pd.DataFrame(updated)
df.to_csv(log_path.joinpath('task_qc_update.csv'))

# For camera QC, we don't store the actual outcomes of the checks in the extended QC, e.g. both PASS and WARNING as
# check results are translated to True in the extended QC
# In order to fully reaggreate the QC, we'd need to recompute the metrics.
# However, in this case there was just a small change in check_wheel_alignment where this test no longer returns
# FAIL but only WARNING. FAIL is encoded in the extended qc as False. So we can set all False on this check to True
# and infer the overall outcome accordingly
dry = True
no_wheel_check = []
int_wheel_check = []
updated = []
# This check always returns None / False for bodyVideo
for video in ['videoLeft', 'videoRight']:
    sess = Session.objects.filter(extended_qc__has_key=video)
    for s in sess:
        if s.extended_qc[video] == 'CRITICAL':
            pass
        else:
            try:
                wheel_check = s.extended_qc[f'_{video}_wheel_alignment']
            except KeyError:
                # In case this check isn't there, QC needs to be rerun
                no_wheel_check.append(str(s.id))
            if isinstance(wheel_check, int):
                # Some sessions have integers as outcomes, not sure what to do with that, probably rerun
                int_wheel_check.append(str(s.id))
            elif wheel_check is None or wheel_check[0] is True:
                # If wheel check had passed or was not set before, nothing changes
                pass
            else:
                # If wheel check outcome was False, check if overall outcome of video needs updating
                results = [v for k, v in s.extended_qc.items() if k.startswith(f'_{video}')]
                results_bool = [v[0] if isinstance(v, list) else v for v in results]
                if any([r is False for r in results_bool]):
                    # Outcome remains
                    new_outcome = False
                else:
                    # Otherwise the new outcome is warning because the wheel will be set to WARNING (which confusingly
                    # results in True in the actual check entry in Alyx
                    new_outcome = True

                # Save all sessions that had their wheel check updated from False to True, also save overall outcome
                updated.append({
                    'session': str(s.id),
                    'video': video[5:],
                    'pre': s.extended_qc[video],
                    'post': s.extended_qc[video] if new_outcome is False else 'WARNING',
                })

                if not dry:
                    # Prevent from hitting the database to fast in a row
                    time.sleep(0.1)
                    s.extended_qc[f'_{video}_wheel_alignment'] = [True, wheel_check[1]]
                    if new_outcome is True:
                        s.extended_qc[video] = 'WARNING'
                    s.save()

df = pd.DataFrame(updated)
# df.to_csv(log_path.joinpath('video_qc_update.csv'))
df_int = pd.DataFrame(columns=['session'], data=int_wheel_check)
df_int.to_csv(log_path.joinpath('wheel_check_int.csv'))

# Fix each DLC QC







# Update overall QC for all sessions
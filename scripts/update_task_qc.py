from actions.models import Session
from ibllib.qc.task_metrics import TaskQC
import pandas as pd
import time
from pathlib import Path

"""
WARNING: 
Don't run this on mbox (has killed the instance in the past), but connect your local alyx to the production RDS, 
run this, disconnect from the RDS (!). Always dry-run first.
"""

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
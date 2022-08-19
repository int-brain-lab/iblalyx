from actions.models import Session
import pandas as pd
import time
from pathlib import Path

dry = True


CRITERIA = {'CRITICAL': 50,
            'FAIL': 40,
            'WARNING': 30,
            'PASS': 10,
            'NOT_SET': 0
            }
REV_CRITERIA = {v:k for k,v in CRITERIA.items()}

log_path = Path.home().joinpath('qc_update')
log_path.mkdir(exist_ok=True, parents=True)

df_task = pd.read_csv(log_path.joinpath('task_qc_update.csv'), index_col=0)
df_video = pd.read_csv(log_path.joinpath('video_qc_update.csv'), index_col=0)
df_dlc = pd.read_csv(log_path.joinpath('dlc_qc_update.csv'), index_col=0)

eids = []
for df in [df_task, df_dlc, df_video]:
    df_check = df[df['pre'] != df['post']]
    eids.extend(list(df_check['session']))

sessions = Session.objects.filter(id__in=eids).distinct()
namespaces = ['task', 'videoLeft', 'videoRight', 'videoBody', 'dlcLeft', 'dlcRight', 'dlcBody']
updated = []
for s in sessions:
    if s.qc == 50:
        pass
    else:
        # Get all possible QCs
        qcs = [CRITERIA[s.extended_qc[k]] for k in namespaces if k in s.extended_qc.keys()]
        new_qc = max(qcs)
        if new_qc != s.qc:
            updated.append({
                'session': str(s.id),
                'pre': REV_CRITERIA[s.qc],
                'post': REV_CRITERIA[new_qc]
            })
            if not dry:
                time.sleep(0.1)
                s.qc = new_qc
                s.save()
df = pd.DataFrame(updated)
df.to_csv(log_path.joinpath('overall_qc_update.csv'))



from actions.models import Session
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

# For camera QC, we don't store the actual outcomes of the checks in the extended QC, but threshold at FAIL (PASS and
# WARNING are both True). To fully reaggreate the QC, we'd need to recompute the metrics.
# In this case there was just a small change in check_wheel_alignment which now never returns FAIL but only PASS or
# WARNING, so we can set all False on this check to True and infer the overall outcome accordingly
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
            if f'_{video}_wheel_alignment' not in s.extended_qc.keys():
                # In case this check isn't there, QC needs to be rerun
                no_wheel_check.append(str(s.id))
            else:
                wheel_check = s.extended_qc[f'_{video}_wheel_alignment']
                if isinstance(wheel_check, int):
                    # Some sessions have integers as outcomes, not sure what to do with that, probably rerun
                    int_wheel_check.append(str(s.id))
                elif wheel_check is None or wheel_check[0] is True:
                    # If wheel check had passed or was not set before, nothing changes
                    pass
                else:
                    # If wheel check outcome was False, check if overall outcome of video needs updating
                    results = [v for k, v in s.extended_qc.items() if
                               k.startswith(f'_{video}') and k != f'_{video}_wheel_alignment']
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
df.to_csv(log_path.joinpath('video_qc_update.csv'))
df_int = pd.DataFrame(columns=['session'], data=int_wheel_check)
df_int.to_csv(log_path.joinpath('wheel_check_int.csv'))

# Fix DLC QC
dry = True
no_pupil_check = []
updated = []
# Only thing that changed is that check_pupil_diameter_snr is no longer counted towards the aggregate. Luckily all
# DLC tests only ever returns PASS or FAIL, so it is easy to recompute the aggregate while leaving one out.
for dlc in ['dlcLeft', 'dlcRight']:
    sess = Session.objects.filter(extended_qc__has_key=dlc)
    for s in sess:
        if s.extended_qc[dlc] == 'CRITICAL':
            pass
        else:
            if f'_{dlc}_pupil_diameter_snr' not in s.extended_qc.keys():
                no_pupil_check.append(str(s.id))
            else:
                pupil_check = s.extended_qc[f'_{dlc}_pupil_diameter_snr']
                if pupil_check is None:
                    # If wheel check was not set before, nothing changes
                    pass
                else:
                    # We need the remaining results to decide what to do with the outcome
                    results = [v for k, v in s.extended_qc.items() if
                               k.startswith(f'_{dlc}') and k != f'_{dlc}_pupil_diameter_snr']
                    results_bool = [v[0] if isinstance(v, list) else v for v in results]
                    # If pupil check was the only True, overall outcome needs to be FAIL now
                    if pupil_check[0] is True and all([r is False for r in results_bool]):
                        updated.append({
                            'session': str(s.id),
                            'dlc': dlc[5:],
                            'pre': s.extended_qc[dlc],
                            'post': 'FAIL',
                        })
                        if not dry:
                            time.sleep(0.1)
                            s.extended_qc[dlc] = 'FAIL'
                            s.save()
                    # If pupil check was the only False, overall outcome needs to be PASS now
                    elif (pupil_check[0] is False and all([r is True for r in results_bool])
                          and s.extended_qc[dlc] == 'FAIL'):
                        updated.append({
                            'session': str(s.id),
                            'dlc': dlc[3:],
                            'pre': s.extended_qc[dlc],
                            'post': 'PASS',
                        })
                        if not dry:
                            time.sleep(0.1)
                            s.extended_qc[dlc] = 'PASS'
                            s.save()
                    else:
                        pass
df = pd.DataFrame(updated)
df.to_csv(log_path.joinpath('dlc_qc_update.csv'))
df_no = pd.DataFrame(columns=['session'], data=no_pupil_check)
df_no.to_csv(log_path.joinpath('no_pupil_check.csv'))

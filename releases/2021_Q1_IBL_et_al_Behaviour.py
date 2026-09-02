import pandas as pd
from django.db.models import Q
from data.models import Tag, Dataset, DatasetType, DataNotice
from actions.models import Session
from subjects.models import Subject

# Releases as part of paper The International Brain Laboratory et al, 2021, DOI: 10.7554/eLife.63711
TAG = '2021_Q1_IBL_et_al_Behaviour'
IBLALYX = '/home/ubuntu/iblalyx'
# Load in the original datasets (this has been renamed to '2021_Q1_IBL_et_al_Behaviour_datasets_v1.pqt')
orig_dsets = pd.read_parquet(f'{IBLALYX}/releases/{TAG}_datasets_v1.pqt')
dsets = Dataset.objects.filter(id__in=orig_dsets['dataset_id'].values)

# Remove datasets from ZFM-01575 sessions, these violate non unique eids
dsets = dsets.exclude(session__subject__nickname='ZFM-01575')

# Add in additional cortex lab sessions that were missing from original release
cortex_lab = ['KS023', 'KS017', 'KS015', 'KS002', 'KS024', 'KS025', 'KS018',
              'KS005', 'KS022', 'KS019', 'KS021', 'KS014', 'KS016', 'KS004']

CUTOFF_DATE = '2020-03-24'  # Date after which sessions are excluded, previously 30th Nov
STABLE_HW_DATE = '2019-06-11'  # Date after which hardware was deemed stable
sessions = (Session.objects.filter(subject__nickname__in=cortex_lab, start_time__gt=STABLE_HW_DATE,
                                   start_time__lt=CUTOFF_DATE, data_dataset_session_related__name__icontains='trials')
            .exclude(task_protocol__icontains='habituation')
            .distinct())

# First get the trials.table datasets for all session that they are available for
tables_ds = Dataset.objects.filter(session__in=sessions, dataset_type__name='trials.table', default_dataset=True)
# For the rest of the sessions, get the individual datasets
sess_no_table = Session.objects.filter(pk__in=sessions).filter(~Q(pk__in=tables_ds.values_list('session_id')))
dtypes = [
            'trials.feedback_times',
            'trials.feedbackType',
            'trials.intervals',
            'trials.choice',
            'trials.response_times',
            'trials.contrastLeft',
            'trials.contrastRight',
            'trials.probabilityLeft',
            'trials.stimOn_times',
            'trials.goCue_times',
            ]
dataset_types = DatasetType.objects.filter(name__in=dtypes)
indv_ds = Dataset.objects.filter(session__in=sess_no_table, dataset_type__in=dataset_types)

# Join all the datasets together
dsets = dsets | tables_ds | indv_ds

# Add in the aggregate subjectTrials and subjectTraining table datasets for 140 subjects used in analysis

subjects = ['NYU-12', 'NYU-14', 'NYU-20', 'IBL-T3', 'IBL-T1', 'IBL-T4',
       'NYU-06', 'NYU-09', 'NYU-11', 'NYU-04', 'NYU-07', 'IBL-T2',
       'NYU-13', 'NYU-02', 'NYU-01', 'CSHL_003', 'CSHL_006', 'CSHL055',
       'CSHL053', 'CSHL_002', 'CSHL_004', 'CSHL060', 'CSHL049',
       'CSHL_005', 'CSHL051', 'CSHL_014', 'CSHL_008', 'CSHL_012',
       'CSHL054', 'CSHL059', 'CSHL_001', 'CSHL_007', 'CSHL052', 'CSHL045',
       'CSHL_015', 'CSHL046', 'CSHL_010', 'CSHL047', 'CSHL058', 'KS020',
       'KS023', 'KS017', 'KS015', 'KS002', 'KS024', 'KS025', 'KS018',
       'KS005', 'KS022', 'KS019', 'KS021', 'KS014', 'KS016', 'KS004',
       'DY_014', 'DY_008', 'DY_001', 'DY_010', 'DY_006', 'DY_011',
       'DY_015', 'DY_003', 'DY_009', 'DY_005', 'DY_013', 'DY_002',
       'DY_007', 'SWC_001', 'SWC_015', 'SWC_013', 'SWC_014', 'SWC_021',
       'SWC_042', 'ZM_2107', 'ZM_1372', 'ZM_2106', 'ZM_1086', 'ZM_1897',
       'ZM_2245', 'ZM_1746', 'ZM_3004', 'ZM_3001', 'ZM_1092', 'ZM_1373',
       'ZM_1369', 'ZM_1097', 'ZM_1093', 'ZM_2241', 'ZM_1367', 'ZM_2240',
       'ZM_1095', 'ZM_1745', 'ZM_1098', 'ZM_3006', 'ZM_1084', 'ZM_1087',
       'ZM_1085', 'ZM_1898', 'ZM_1089', 'ZM_1371', 'ZM_3002', 'ZM_3003',
       'ZM_1743', 'ZM_1091', 'ZM_1928', 'SWC_029', 'SWC_038', 'SWC_023',
       'IBL_001', 'SWC_017', 'SWC_022', 'SWC_030', 'SWC_018', 'IBL_002',
       'SWC_039', 'ibl_witten_03', 'ibl_witten_07', 'ibl_witten_17',
       'ibl_witten_19', 'ibl_witten_06', 'ibl_witten_12', 'ibl_witten_20',
       'ibl_witten_16', 'ibl_witten_13', 'ibl_witten_04', 'ibl_witten_15',
       'ibl_witten_02', 'ibl_witten_14', 'ibl_witten_05', 'CSH_ZAD_010',
       'CSH_ZAD_006', 'CSH_ZAD_004', 'CSH_ZAD_022', 'CSH_ZAD_017',
       'CSH_ZAD_011', 'CSH_ZAD_007', 'CSH_ZAD_001', 'CSH_ZAD_002',
       'CSH_ZAD_003', 'CSH_ZAD_005']


subj = Subject.objects.filter(nickname__in=subjects)
agg_trials = Dataset.objects.filter(object_id__in=subj, name='_ibl_subjectTrials.table.pqt', default_dataset=True)
agg_training = Dataset.objects.filter(object_id__in=subj, name='_ibl_subjectTraining.table.pqt', default_dataset=True)
agg_sessions = Dataset.objects.filter(object_id__in=subj, name='_ibl_subjectSessions.table.pqt', default_dataset=True)

dsets = dsets | agg_trials | agg_training | agg_sessions
dsets = dsets.distinct()

# Tagging in production database
tag, _ = Tag.objects.get_or_create(name=TAG, protected=True, public=True)
tag.datasets.set(dsets)

# Saving dataset IDs for release in the public database
dset_ids = [str(eid) for eid in dsets.values_list('pk', flat=True)]
df = pd.DataFrame(dset_ids, columns=['dataset_id'])
df.to_parquet(f'{IBLALYX}/releases/{TAG}_datasets.pqt')

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""
Adapted code to add wheel and wheelMoves datasets Aug 2026

On 2026-08-28 it was decided that we set the dataset QC for
for wheel and wheelMoves datasets before release.

For sessions that have a wheel dataset, we will check the
extended_qc and set datasets to PASS if all wheel related
QC metrics pass for >= 0.95, otherwise FAIL (or NOT_SET)

As we are adding new datasets to the release, we don't need
to create a new tag as it's unlikely to affect reproduction
of the paper's analysis.
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""

# Load in the previous datasets list (this has been renamed to
# '2021_Q1_IBL_et_al_Behaviour_datasets_v2.pqt')
orig_dsets = pd.read_parquet(f'{IBLALYX}/releases/{TAG}_datasets_v2.pqt')
dsets = Dataset.objects.filter(id__in=orig_dsets['dataset_id'].values)

# Find sessions with the tag and check their extended_qc
sessions = Session.objects.filter(data_dataset_session_related__in=dsets).distinct().values('pk', 'extended_qc')
print(f"Found {sessions.count()} sessions with tag {TAG}")

from collections import defaultdict
from one.alf.spec import QC
wheel_qc_map = defaultdict(list)  # Outcome map for wheel datasets, keys are QC.PASS, QC.FAIL, QC.NOT_SET
trials_qc_map = defaultdict(list)  # Outcome map for trials.firstMovement_times dataset, keys are QC.PASS, QC.FAIL, QC.NOT_SET
for s in sessions:
    # First check there are at least the wheel timestamps and position datasets for this session
    if not (Dataset.objects.filter(session_id=s['pk'], name__startswith='_ibl_wheel.timestamps').exists()
            and Dataset.objects.filter(session_id=s['pk'], name__startswith='_ibl_wheel.position').exists()):
        print(f"Session {s['pk']} does not have wheel datasets")
        continue
    if not s['extended_qc']:
        print(f"Session {s['pk']} has no extended_qc")
        wheel_qc_map[QC.NOT_SET].append(s['pk'])
        trials_qc_map[QC.NOT_SET].append(s['pk'])
        continue
    extended_qc = {k: v for k, v in s['extended_qc'].items()
                   if k.startswith('_task') and 'wheel' in k}
    if len(extended_qc.values()) == 0 or all(v is None for v in extended_qc.values()):
        print(f"Session {s['pk']} has no wheel related extended_qc")
        wheel_qc_map[QC.NOT_SET].append(s['pk'])
        trials_qc_map[QC.NOT_SET].append(s['pk'])
        continue
    passed = True
    for k, v in extended_qc.items():
        if v is None:
            print(f"Session {s['pk']} failed {k} with value {v}")
            if 'detected_wheel_moves' in k:
                trials_qc_map[QC.NOT_SET].append(s['pk'])
                continue
            passed = False
            break
        if v < 0.95:
            print(f"Session {s['pk']} failed {k} with value {v}")
            if 'detected_wheel_moves' in k:
                trials_qc_map[QC.FAIL].append(s['pk'])
                continue
            passed = False
            break
        if 'detected_wheel_moves' in k:
            trials_qc_map[QC.PASS].append(s['pk'])
    if passed:
        wheel_qc_map[QC.PASS].append(s['pk'])
    else:
        wheel_qc_map[QC.FAIL].append(s['pk'])

to_tag = set()
# Bulk update the QC for wheel datasets
for qc, session_ids in wheel_qc_map.items():
    wheel_dsets = Dataset.objects.filter(session_id__in=session_ids, name__startswith='_ibl_wheel', default_dataset=True)
    wheel_dsets.update(qc=qc)
    print(f"Set {len(wheel_dsets)} wheel datasets to QC {qc}")
    to_tag.update(wheel_dsets)
for qc, session_ids in trials_qc_map.items():
    trials_dsets = Dataset.objects.filter(session_id__in=session_ids, name__startswith='_ibl_trials.firstMovement_times', default_dataset=True)
    trials_dsets.update(qc=qc)
    print(f"Set {len(trials_dsets)} trials.firstMovement_times datasets to QC {qc}")
    to_tag.update(trials_dsets)

# tag.datasets.set() only manages this tag's membership, other tags already on
# these datasets (e.g. brainwide map releases) are left untouched
tag, _ = Tag.objects.get_or_create(name=TAG, protected=True, public=True)
tag.datasets.set(to_tag | set(dsets))

# Create a DataNotice for the wheel datasets that have been added to the release
notice_text = f"""
# Behaviour paper sessions wheel datasets

The following wheel dataset types have been added to the {TAG} release:
- wheel.timestamps
- wheel.position
- wheelMoves.intervals
- wheelMoves.peakAmplitude
- trials.firstMovement_times

The QC for these datasets has been set based on the extended_qc metrics for each session.
The QC for wheel datasets is set to PASS if all wheel related extended_qc metrics pass for >= 95% of trials,
otherwise FAIL (or NOT_SET if the metrics are missing).

## Wheel related extended_qc metrics

### wheel_integrity
Check wheel position sampled at the expected resolution.

### check_detected_wheel_moves
Check that the detected first movement times are reasonable. (This metric is used to set the QC for trials.firstMovement_times datasets.)

### wheel_move_before_feedback:
Check that the wheel does move within 100ms of the feedback onset (error sound or valve).

### wheel_move_during_closed_loop
Check the wheel moves the correct amount to reach threshold.

### wheel_freeze_during_quiescence
Check the wheel is indeed still during the quiescent period.

**NB**: Several of these metrics most often fail due to inaccurate or missing stimulus timestamps,
 however given that such events are generally required for wheel analysis, we will set the QC to
 FAIL for wheel datasets if any of these metrics fail.
"""
notice = DataNotice.objects.create(
    name='Behaviour paper sessions wheel datasets',
    importance=DataNotice.IMPORTANCE.INSIGNIFICANT,
    description=notice_text
)
notice.datasets.set(to_tag)

# Saving dataset IDs and session IDs for release in the public database
df = pd.DataFrame(Dataset.objects.filter(tags=tag).values('pk', 'session')).astype(str)
df.columns = ['dataset_id', 'session_id']
df.to_parquet(f'{IBLALYX}/releases/{TAG}_datasets.pqt')


"""
Over the course of adding these wheel datasets a number of datasets were found
to be missing (not present on flatiron or aws). These have been untagged and removed.
It's unclear if these ever existed on flatiron.
"""
from pathlib import Path
from uuid import UUID
IBL_DEV_TOOLS = Path.home().joinpath('Documents', 'PYTHON', 'ibldevtools')
missing_df = pd.read_parquet(IBL_DEV_TOOLS / 'miles/missing_behaviour_datasets.pqt')
missing_ids = missing_df['id'].apply(lambda x: UUID(int=int.from_bytes(x, 'big'))).astype(str).values
dsets = pd.read_parquet(f'{IBLALYX}/releases/{TAG}_datasets.pqt')
dsets = dsets[~dsets['dataset_id'].isin(missing_ids)]
dsets.to_parquet(f'{IBLALYX}/releases/{TAG}_datasets.pqt')

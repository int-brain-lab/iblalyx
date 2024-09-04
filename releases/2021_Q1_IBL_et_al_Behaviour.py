import pandas as pd
from django.db.models import Q
from data.models import Tag, Dataset, DatasetType
from actions.models import Session
from subjects.models import Subject

# Releases as part of paper The International Brain Laboratory et al, 2021, DOI: 10.7554/eLife.63711

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

# To keep consistent with previous behavior paper release. Trials from these subjects were also published
additional_subjects = ['CSHL034', 'CSHL056', 'CSHL057', 'CSHL_011', 'CSHL_013', 'CSH_ZAD_009',
                       'CSH_ZAD_015', 'CSH_ZAD_016', 'CSH_ZAD_018', 'CSH_ZAD_019', 'CSH_ZAD_021',
                       'CSH_ZAD_023', 'CSH_ZAD_024', 'DY_012', 'IBL_005', 'NYU-21', 'NYU-23', 'NYU-24',
                       'NYU-25', 'NYU-26', 'SWC_002', 'SWC_003', 'SWC_004', 'SWC_006', 'SWC_007', 'SWC_010',
                       'SWC_011', 'SWC_016', 'SWC_019', 'SWC_020', 'SWC_027', 'SWC_028', 'SWC_032', 'SWC_033',
                       'SWC_034', 'SWC_035', 'SWC_036', 'SWC_040', 'SWC_041', 'SWC_043', 'ZM_3005',
                       'ibl_witten_11', 'ibl_witten_18', 'ibl_witten_21', 'ibl_witten_22', 'ibl_witten_23', 'ibl_witten_24']

# N.B removed 'ZFM-01575' from additional subjects. Has eid to path unique violation so better not to release

use_extra = True
query_subjects = subjects + additional_subjects if use_extra else subjects

CUTOFF_DATE = '2020-03-24'  # Date after which sessions are excluded, previously 30th Nov
STABLE_HW_DATE = '2019-06-11'  # Date after which hardware was deemed stable
sessions = (Session.objects.filter(subject__nickname__in=query_subjects, start_time__gt=STABLE_HW_DATE,
                                   start_time__lt=CUTOFF_DATE, data_dataset_session_related__name__icontains='trials')
            .exclude(task_protocol__icontains='habituation')
            .distinct())

# Save the sessions to csv
eids = [str(sess.id) for sess in sessions]
df = pd.DataFrame()
df['session_id'] = eids
df.to_csv('2021_Q1_IBL_et_al_Behaviour_sessions.csv')


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

# For all sessions, get the reNum data (not included in trials.table)
# Do we want to release these?
repnum_ds = Dataset.objects.filter(session__in=sessions, dataset_type__name='trials.repNum')

# Get the aggregate datasets for the subjects
subj = Subject.objects.filter(nickname__in=subjects)
agg_trials = Dataset.objects.filter(object_id=subj, name='_ibl_subjectTrials.table.pqt', default_dataset=True)
agg_training = Dataset.objects.filter(object_id=subj, name='_ibl_subjectTraining.table.pqt', default_dataset=True)

# Bring all datasets together
dsets = tables_ds | indv_ds | repnum_ds | agg_trials | agg_training

# Tagging in production database
tag, _ = Tag.objects.get_or_create(name="2021_Q1_IBL_et_al_Behaviour", protected=True, public=True)
tag.datasets.set(dsets)

# Saving dataset IDs for release in the public database
dset_ids = [str(eid) for eid in dsets.values_list('pk', flat=True)]
df = pd.DataFrame(dset_ids, columns=['dataset_id'])
df.to_parquet('./2021_Q1_IBL_et_al_Behaviour_datasets.pqt')

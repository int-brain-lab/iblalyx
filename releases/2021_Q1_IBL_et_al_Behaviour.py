import pandas as pd
from django.db.models import Q
from data.models import Tag, Dataset, DatasetType
from actions.models import Session
from subjects.models import Subject

# Releases as part of paper The International Brain Laboratory et al, 2021, DOI: 10.7554/eLife.63711

# Load in the original datasets (this has been renamed to '2021_Q1_IBL_et_al_Behaviour_datasets_v1.pqt')
orig_dsets = pd.read_parquet('/home/ubuntu/iblalyx/releases/2021_Q1_IBL_et_al_Behaviour_datasets_v1.pqt')
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
tag, _ = Tag.objects.get_or_create(name="2021_Q1_IBL_et_al_Behaviour", protected=True, public=True)
tag.datasets.set(dsets)

# Saving dataset IDs for release in the public database
dset_ids = [str(eid) for eid in dsets.values_list('pk', flat=True)]
df = pd.DataFrame(dset_ids, columns=['dataset_id'])
df.to_parquet('/home/ubuntu/iblalyx/releases/2021_Q1_IBL_et_al_Behaviour_datasets.pqt')

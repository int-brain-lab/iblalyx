import pandas as pd
from data.models import Dataset, Tag
from subjects.models import Subject

subjects = [
    'CSHL_015', 'CSHL_014', 'CSHL049', 'CSHL060', 'CSH_ZAD_019', 'CSH_ZAD_017', 'CSHL047', 'CSH_ZAD_011', 'CSHL045',
    'CSHL_020', 'CSHL055', 'CSHL054', 'CSH_ZAD_029', 'CSH_ZAD_001', 'CSHL053', 'CSHL051', 'CSH_ZAD_022', 'CSH_ZAD_026',
    'CSH_ZAD_025', 'CSHL058', 'CSH_ZAD_024', 'CSHL059', 'CSHL_007', 'DY_009', 'DY_008', 'DY_020', 'DY_018', 'DY_011',
    'DY_010', 'DY_013', 'DY_014', 'DY_016', 'ibl_witten_29', 'ibl_witten_25', 'ibl_witten_26', 'ibl_witten_27',
    'ibl_witten_19', 'ibl_witten_14', 'ibl_witten_16', 'ibl_witten_17', 'ibl_witten_13', 'KS022', 'KS091', 'KS023',
    'KS055', 'KS021', 'KS094', 'KS051', 'KS052', 'KS096', 'KS016', 'KS017', 'KS014', 'KS015', 'KS084', 'KS043', 'KS086',
    'KS042', 'KS044', 'KS019', 'KS046', 'NR_0027', 'NR_0019', 'NYU-06', 'NR_0020', 'NYU-27', 'NYU-47', 'NYU-30',
    'NYU-46', 'NYU-45', 'NYU-37', 'NYU-40', 'NYU-11', 'NYU-39', 'NR_0021', 'NYU-48', 'PL017', 'PL024', 'SWC_058',
    'SWC_022', 'SWC_054', 'SWC_023', 'SWC_021', 'SWC_043', 'SWC_042', 'SWC_060', 'SWC_061', 'SWC_066', 'SWC_038',
    'SWC_039', 'UCLA017', 'UCLA014', 'UCLA037', 'UCLA012', 'UCLA011', 'UCLA035', 'UCLA033', 'UCLA034', 'UCLA036',
    'UCLA006', 'UCLA005', 'ZFM-01577', 'ZFM-05236', 'ZFM-01576', 'ZFM-01937', 'ZFM-01936', 'ZFM-01935', 'ZM_3003',
    'ZFM-02372', 'ZFM-02373', 'ZFM-01592', 'ZFM-02370', 'ZM_1897', 'ZM_2245', 'ZFM-02368', 'ZFM-02369', 'ZM_1898',
    'ZM_2241', 'ZM_2240'
]

subjects = Subject.objects.filter(nickname__in=subjects)

datasets = Dataset.objects.filter(name__in=['_ibl_subjectTrials.table.pqt', '_ibl_subjectTraining.table.pqt']
                                  , object_id__in=subjects.values_list('pk', flat=True))

tag, _ = Tag.objects.get_or_create(name="2023_Q4_Bruijns_et_al", protected=True, public=True)
tag.datasets.set(datasets)

# Save dataset IDs for release in public database
dset_ids = [str(eid) for eid in datasets.values_list('pk', flat=True)]
df = pd.DataFrame(dset_ids, columns=['dataset_id'])
df.to_parquet('./2023_Q4_Bruijns_et_al_datasets.pqt')

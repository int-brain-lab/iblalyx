import pandas as pd
from data.models import Dataset, Tag
from subjects.models import Subject

subjects = ['CSHL045', 'CSHL047', 'CSHL049', 'CSHL051', 'CSHL052', 'CSHL053', 'CSHL054', 'CSHL055', 'CSHL058',
            'CSHL059', 'CSHL060', 'CSHL_007', 'CSHL_014', 'CSHL_015', 'CSHL_020', 'CSH_ZAD_001', 'CSH_ZAD_011',
            'CSH_ZAD_017', 'CSH_ZAD_019', 'CSH_ZAD_022', 'CSH_ZAD_024', 'CSH_ZAD_025', 'CSH_ZAD_026', 'CSH_ZAD_029',
            'DY_008', 'DY_009', 'DY_010', 'DY_011', 'DY_013', 'DY_014', 'DY_016', 'DY_018', 'DY_020', 'KS014', 'KS015',
            'KS016', 'KS017', 'KS019', 'KS021', 'KS022', 'KS023', 'KS042', 'KS043', 'KS044', 'KS045', 'KS046', 'KS051',
            'KS052', 'KS055', 'KS084', 'KS086', 'KS091', 'KS094', 'KS096', 'MFD_05', 'MFD_06', 'MFD_07', 'MFD_08',
            'MFD_09', 'NR_0017', 'NR_0019', 'NR_0020', 'NR_0021', 'NR_0024', 'NR_0027', 'NR_0028', 'NR_0029',
            'NR_0031', 'NYU-06', 'NYU-11', 'NYU-12', 'NYU-21', 'NYU-27', 'NYU-30', 'NYU-37', 'NYU-39', 'NYU-40',
            'NYU-45', 'NYU-46', 'NYU-47', 'NYU-48', 'NYU-65', 'PL015', 'PL016', 'PL017', 'PL024', 'PL030', 'PL031',
            'PL033', 'PL034', 'PL035', 'PL037', 'PL050', 'SWC_021', 'SWC_022', 'SWC_023', 'SWC_038', 'SWC_039',
            'SWC_042', 'SWC_043', 'SWC_052', 'SWC_053', 'SWC_054', 'SWC_058', 'SWC_060', 'SWC_061', 'SWC_065',
            'SWC_066', 'UCLA005', 'UCLA006', 'UCLA011', 'UCLA012', 'UCLA014', 'UCLA015', 'UCLA017', 'UCLA030',
            'UCLA033', 'UCLA034', 'UCLA035', 'UCLA036', 'UCLA037', 'UCLA044', 'UCLA048', 'UCLA049', 'UCLA052',
            'ZFM-01576', 'ZFM-01577', 'ZFM-01592', 'ZFM-01935', 'ZFM-01936', 'ZFM-01937', 'ZFM-02368', 'ZFM-02369',
            'ZFM-02370', 'ZFM-02372', 'ZFM-02373', 'ZFM-04308', 'ZFM-05236', 'ZM_1897', 'ZM_1898', 'ZM_2240',
            'ZM_2241', 'ZM_2245', 'ZM_3003', 'ibl_witten_13', 'ibl_witten_14', 'ibl_witten_16', 'ibl_witten_17',
            'ibl_witten_18', 'ibl_witten_19', 'ibl_witten_20', 'ibl_witten_25', 'ibl_witten_26', 'ibl_witten_27',
            'ibl_witten_29', 'ibl_witten_32'
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

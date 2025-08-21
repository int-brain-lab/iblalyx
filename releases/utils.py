"""
Utils function to re-use in the release process
Here is how I link it to my current interpreter, you can replace IBL_ALYX_ROOT with your actual path

    import alyx.base
    IBL_ALYX_ROOT = Path(alyx.base.__file__).parents[3].joinpath('iblalyx')
    assert IBL_ALYX_ROOT.exists(), 'No IBL_ALYX_ROOT found, it is usually at the same directory level as the alyx repo'
    sys.path.append(str(IBL_ALYX_ROOT.parent))
"""

import pandas as pd

from actions.models import Session
from data.models import Dataset
from django.db.models import Q

"""
Settings and Inputs
the parquet files names match exactly the tag name in the database
"""

PUBLIC_DS_FILES = ['2021_Q1_IBL_et_al_Behaviour_datasets.pqt',
                   '2021_Q2_Varol_et_al_datasets.pqt',
                   '2021_Q3_Whiteway_et_al_datasets.pqt',
                   '2021_Q2_PreRelease_datasets.pqt',
                   '2022_Q2_IBL_et_al_RepeatedSite_datasets.pqt',
                   '2022_Q3_IBL_et_al_DAWG_datasets.pqt',
                   '2022_Q4_IBL_et_al_BWM_datasets.pqt',
                   '2023_Q1_Mohammadi_et_al_datasets.pqt',
                   '2023_Q1_Biderman_Whiteway_et_al_datasets.pqt',
                   '2023_Q3_Findling_Hubert_et_al_datasets.pqt',
                   '2023_Q4_Bruijns_et_al_datasets.pqt',
                   '2023_Q4_IBL_et_al_BWM_2_datasets.pqt',
                   '2023_Q4_IBL_et_al_BWM_passive_datasets.pqt',
                   '2024_Q2_IBL_et_al_BWM_iblsort_datasets.pqt',
                   '2024_Q2_IBL_et_al_RepeatedSite_datasets.pqt',
                   '2024_Q2_Blau_et_al_datasets.pqt',
                   '2024_Q3_Pan_Vazquez_et_al_datasets.pqt',
                   '2025_Q1_IBL_et_al_BWM_wheel_patch_datasets.pqt',
                   '2025_Q3_Meijer_et_al_serotonin.pqt',
                   '2025_Q3_IBL_et_al_BWM.pqt',
                   '2025_Q3_Davatolhagh_et_al_autism_datasets.pqt',
                   ]

PUBLIC_DS_TAGS = [
    "cfc4906a-316e-4150-8222-fe7e7f13bdac",  # "Behaviour Paper", "2021_Q1_IBL_et_al_Behaviour"
    "9dec1de8-389d-40f6-b00b-763e4fda6552",  # "Erdem's paper", "2021_Q2_Varol_et_al"
    "c8f0892a-a95b-4181-b8e6-d5d31cb97449",  # "Matt's paper", "2021_Q3_Whiteway_et_al"
    "dcd8b2e5-3a32-41b4-ac15-085a208a4466",  # "May 2021 pre-release", "2021_Q2_PreRelease"
    "05fbaa4e-681d-41c5-ae53-072cb96f4c0a",  # 2022_Q2_IBL_et_al_RepeatedSite_datasets
    "1f2c5034-b31b-4c23-be93-c4a66c8c9eb1",  # 2022_Q3_IBL_et_al_DAWG
    "4df91a7a-faac-4894-800e-b306cafe9a8c",  # 2022_Q4_IBL_et_al_BWM
    "9cba03f8-f491-43b2-9686-45309aee8657",  # 2023_Q1_Mohammadi_et_al
    "4984ea79-b162-49cc-8660-0dc6bbb7a5ff",  # 2023_Q1_Biderman_Whiteway_et_al
    "0f146f03-8dfe-44d6-84ef-e8fd24762fb2",  # 2023_Q3_Findling_Hubert_et_al
    "a8f643f2-0b71-430f-a57a-485202fba2c1",  # 2023_Q4_Bruijns_et_al
    "7f7cd406-bb25-470f-bae4-55b56c3acac5",  # 2023_Q4_IBL_et_al_BWM_2
    "30734650-65f3-4653-a059-0687ae872c97",  # 2023_Q4_IBL_et_al_BWM_passive
    "66e0eec0-4ecf-4de8-a84c-a2bd8eda06f4",  # 2024_Q2_IBL_et_al_BWM_iblsort
    "9fc1593a-c3c1-4dea-8473-0948ed6a2904",  # 2024_Q2_IBL_et_al_RepeatedSite
    "6828217e-6ae0-44ce-9c6a-bd30f6e523a6",  # 2024_Q2_Blau_et_al
    "89b582ed-54d1-4b03-96a7-9ddb369cd07d",  # 2024_Q3_Pan_Vazquez_et_al
    "3faeb797-0d60-4595-86f4-2712265e6291",  # 2025_Q1_IBL_et_al_BWM_wheel_patch
    "c94baa2d-d627-4a4e-a5c1-eac7efbf644e",  # 2025_Q3_Meijer_et_al
    "60381f40-ef53-4a83-9a36-ef6548f7e996",  # 2025_Q3_IBL_et_al_BWM.pqt
    "7b16fa4f-f759-4181-bdff-779b7fe4a9a6",  # 2025_Q3_Davatolhagh_et_al_autism
]


DTYPES_RELEASE_BEHAVIOUR = [
    '_ibl_trials.laserStimulation',
    '_ibl_trials.quiescencePeriod',
    '_ibl_trials.stimOffTrigger_times',
    '_ibl_trials.stimOnTrigger_times',
    'trials.goCueTrigger_times',
    'trials.intervals',
    'trials.laserProbability',
    'trials.stimOff_times',
    'trials.table',
    'wheel.position',
    'wheel.timestamps',
    'wheelMoves.intervals',
    'wheelMoves.peakAmplitude',
    ]

DTYPES_RELEASE_EPHYS_RAW = [
    '_iblqc_ephysChannels.RMS',
    '_iblqc_ephysChannels.labels',
    '_iblqc_ephysChannels.rawSpikeRates',
    '_iblqc_ephysSpectralDensity.freqs',
    '_iblqc_ephysSpectralDensity.power',
    '_iblqc_ephysTimeRms.rms',
    '_iblqc_ephysTimeRms.timestamps',
    '_iblqc_ephysSaturation.samples',
    'ephysData.raw.ch',
    'ephysData.raw.lf',
    'ephysData.raw.meta',
    'ephysData.raw.sync',
    'ephysData.raw.timestamps',
    'ephysData.raw.wiring',
]

DTYPES_RELEASE_SPIKE_SORTING = [
    '_phy_spikes_subset.channels',
    '_phy_spikes_subset.spikes',
    '_phy_spikes_subset.waveforms',
    '_spikeglx_sync.channels',
    '_spikeglx_sync.polarities',
    '_spikeglx_sync.times',
    'channels.brainLocationIds_ccf_2017',
    'channels.localCoordinates',
    'channels.mlapdv',
    'channels.rawInd',
    'clusters.amps',
    'clusters.channels',
    'clusters.depths',
    'clusters.metrics',
    'clusters.peakToTrough',
    'clusters.uuids',
    'clusters.waveforms',
    'clusters.waveformsChannels',
    'kilosort.whitening_matrix',
    'spikes.amps',
    'spikes.clusters',
    'spikes.depths',
    'spikes.samples',
    'spikes.templates',
    'spikes.times',
    'templates.amps',
    'templates.waveforms',
    'templates.waveformsChannels',
    'waveforms.channels',
    'waveforms.table',
    'waveforms.templates',
    'waveforms.traces',
]

DTYPES_RELEASE_HISTOLOGY = [
    'electrodeSites.brainLocationIds_ccf_2017',
    'electrodeSites.localCoordinates',
    'electrodeSites.mlapdv'
]

DTYPES_RELEASE_EPHYS_ALL = DTYPES_RELEASE_EPHYS_RAW + DTYPES_RELEASE_SPIKE_SORTING + DTYPES_RELEASE_HISTOLOGY


def dset2df(dsets_queryset, columns: dict = None):
    columns = columns if columns is not None else {'id': 'dataset_id', 'session': 'eid', 'collection': 'collection', 'name': 'file', 'dataset_type__name': 'dataset_type'}
    df_datasets = pd.DataFrame(dsets_queryset.values_list(*list(columns.keys())), columns=list(columns.values()))
    for col in ['dataset_id', 'eid']:
        df_datasets[col] = df_datasets[col].astype(str)
    return df_datasets


def get_video_datasets_for_ephys_sessions(eids, cam_labels=None):

    def _get_video_dsets_names(label):
        dnames = [
            f'_ibl_{label}Camera.lightningPose.pqt',
            f'_ibl_{label}Camera.dlc.pqt',
            f'_ibl_{label}Camera.times.npy',
            f'_ibl_{label}Camera.features.pqt',
            f'{label}Camera.ROIMotionEnergy.npy',
            f'{label}ROIMotionEnergy.position.npy',
            f'_iblrig_{label}Camera.raw.mp4'
        ]
        return dnames

    cam_labels = ['left', 'right', 'body'] if cam_labels is None else cam_labels
    # Video datasets
    qs_sess = Session.objects.filter(id__in=eids)
    # Make sure only non-critical video datasets are released
    # Get the video datasets
    dsets_video = Dataset.objects.none()
    for cam in cam_labels:
        field_name = f"extended_qc__video{cam.capitalize()}"
        video_eids = qs_sess.exclude(**{field_name: 'CRITICAL'})
        dnames = _get_video_dsets_names(cam)
        dsets = Dataset.objects.filter(session__in=video_eids, name__in=dnames, default_dataset=True).distinct()
        dsets_video = dsets_video | dsets

    # Get the lick datasets
    # If both left and right are critical we do not release the licks
    lick_eids = qs_sess.exclude(Q(extended_qc__videoLeft='CRITICAL') & Q(extended_qc__videoRight='CRITICAL'))
    dsets = (Dataset.objects.filter(session__in=lick_eids, name='licks.times.npy', default_dataset=True)
             .exclude(tags__name__icontains='brainwide')).distinct()

    dsets_video = dsets_video | dsets
    return dsets_video

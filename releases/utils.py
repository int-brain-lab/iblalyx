"""
Utils function to re-use in the release process
Here is how I link it to my current interpreter, you can replace IBL_ALYX_ROOT with your actual path

    import alyx.base
    IBL_ALYX_ROOT = Path(alyx.base.__file__).parents[3].joinpath('iblalyx')
    assert IBL_ALYX_ROOT.exists(), 'No IBL_ALYX_ROOT found, it is usually at the same directory level as the alyx repo'
    sys.path.append(str(IBL_ALYX_ROOT.parent))
"""
from actions.models import Session
from data.models import Dataset
from django.db.models import Q


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
    'electrodeSites.brainLocationIds_ccf_2017',
    'electrodeSites.localCoordinates',
    'electrodeSites.mlapdv',
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

DTYPES_RELEASE_EPHYS_ALL = DTYPES_RELEASE_EPHYS_RAW + DTYPES_RELEASE_SPIKE_SORTING


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

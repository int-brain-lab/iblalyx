"""
Data Release request link:
https://docs.google.com/document/d/1IzZ79zYu9p8cpIGk2cuNQn__7VZZctWCPcwrXCiAD7g/edit?tab=t.0

"""
import sys, os
from pathlib import Path
from itertools import groupby

import tqdm
import pandas as pd
import django

if __name__ == '__main__' and not os.environ.get('DJANGO_SETTINGS_MODULE'):
    sys.path.insert(0, '.')
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'alyx.settings')
    django.setup()

from data.models import Dataset, Tag

# from one_django import OneDjango
# one = OneDjango()
DRY_RUN = True
TAG_NAME = '2026_Q3_Großmann_et_al'
IBL_ALYX_ROOT = Path(__file__).parent

EIDS = [
    '09f98d3f-78bc-4685-8d80-20f3b58cb152',
    '1f938401-8ba7-4392-9c24-577db4ada2c4',
    '4bf62d04-e5a6-46db-add6-35e55db2de71',
    '4ffa0b64-1867-4d56-814c-60eee3c77d40',
    '5531e71f-8ab9-4c4e-8d5b-d92da838ee16',
    '70f666a1-16a3-4973-883f-8b2beaee2e12',
    '73fa004c-eac3-4b34-997a-9b4268990474',
    '8840ef53-d5c1-44dd-b91f-736276bddddd',
    '8f604df7-ea3c-48de-ad31-b6c532989e6a',
    '9ee8b642-9d7f-483f-93b4-5b2ed62c7653',
    'a59df7cb-bf0e-4160-993c-21de8aea6126',
    'a5f5eacc-3344-4f24-a41a-a6018c1eabf1',
    'ae8ee6df-d1b8-4700-aa18-0da469e22d36',
    'c211d400-04aa-479f-9544-3d30e29b8c5f',
    'cc8ecfbb-ba55-4bcf-a7b5-d96b41c0ca49',
    'cc9d19f3-c389-462d-86d9-1ea739ff2669',
    'd29aad5f-d719-4a82-88e8-0f5bdac09fbe',
    'eaf068bc-f879-44c4-930b-ef2e5cb1d9e4',
    'f5ae99d3-7578-4766-b005-f2302bc52808',
    'fea9173d-8609-4242-9369-308fa29bf0ac',
    'Fee5810d-2012-4337-84f9-dce8e005ea10'
]


datasets_relative_paths = [
    "alf/probe00/electrodeSites.brainLocationIds_ccf_2017.npy",
    "alf/probe00/electrodeSites.localCoordinates.npy",
    "alf/probe00/electrodeSites.mlapdv.npy",
    "alf/probe00/pykilosort/_kilosort_whitening.matrix.npy",
    "alf/probe00/pykilosort/_phy_spikes_subset.channels.npy",
    "alf/probe00/pykilosort/_phy_spikes_subset.spikes.npy",
    "alf/probe00/pykilosort/_phy_spikes_subset.waveforms.npy",
    "alf/probe00/pykilosort/channels.brainLocationIds_ccf_2017.npy",
    "alf/probe00/pykilosort/channels.localCoordinates.npy",
    "alf/probe00/pykilosort/channels.mlapdv.npy",
    "alf/probe00/pykilosort/channels.rawInd.npy",
    "alf/probe00/pykilosort/clusters.amps.npy",
    "alf/probe00/pykilosort/clusters.channels.npy",
    "alf/probe00/pykilosort/clusters.depths.npy",
    "alf/probe00/pykilosort/clusters.metrics.pqt",
    "alf/probe00/pykilosort/clusters.peakToTrough.npy",
    "alf/probe00/pykilosort/clusters.uuids.csv",
    "alf/probe00/pykilosort/clusters.waveforms.npy",
    "alf/probe00/pykilosort/clusters.waveformsChannels.npy",
    "alf/probe00/pykilosort/spikes.amps.npy",
    "alf/probe00/pykilosort/spikes.clusters.npy",
    "alf/probe00/pykilosort/spikes.depths.npy",
    "alf/probe00/pykilosort/spikes.samples.npy",
    "alf/probe00/pykilosort/spikes.templates.npy",
    "alf/probe00/pykilosort/spikes.times.npy",
    "alf/probe00/pykilosort/templates.amps.npy",
    "alf/probe00/pykilosort/templates.waveforms.npy",
    "alf/probe00/pykilosort/templates.waveformsChannels.npy",
    "alf/probe01/electrodeSites.brainLocationIds_ccf_2017.npy",
    "alf/probe01/electrodeSites.localCoordinates.npy",
    "alf/probe01/electrodeSites.mlapdv.npy",
    "alf/probe01/pykilosort/_kilosort_whitening.matrix.npy",
    "alf/probe01/pykilosort/_phy_spikes_subset.channels.npy",
    "alf/probe01/pykilosort/_phy_spikes_subset.spikes.npy",
    "alf/probe01/pykilosort/_phy_spikes_subset.waveforms.npy",
    "alf/probe01/pykilosort/channels.brainLocationIds_ccf_2017.npy",
    "alf/probe01/pykilosort/channels.localCoordinates.npy",
    "alf/probe01/pykilosort/channels.mlapdv.npy",
    "alf/probe01/pykilosort/channels.rawInd.npy",
    "alf/probe01/pykilosort/clusters.amps.npy",
    "alf/probe01/pykilosort/clusters.channels.npy",
    "alf/probe01/pykilosort/clusters.depths.npy",
    "alf/probe01/pykilosort/clusters.metrics.pqt",
    "alf/probe01/pykilosort/clusters.peakToTrough.npy",
    "alf/probe01/pykilosort/clusters.uuids.csv",
    "alf/probe01/pykilosort/clusters.waveforms.npy",
    "alf/probe01/pykilosort/clusters.waveformsChannels.npy",
    "alf/probe01/pykilosort/spikes.amps.npy",
    "alf/probe01/pykilosort/spikes.clusters.npy",
    "alf/probe01/pykilosort/spikes.depths.npy",
    "alf/probe01/pykilosort/spikes.samples.npy",
    "alf/probe01/pykilosort/spikes.templates.npy",
    "alf/probe01/pykilosort/spikes.times.npy",
    "alf/probe01/pykilosort/templates.amps.npy",
    "alf/probe01/pykilosort/templates.waveforms.npy",
    "alf/probe01/pykilosort/templates.waveformsChannels.npy",
    "raw_ephys_data/_spikeglx_sync.channels.npy",
    "raw_ephys_data/_spikeglx_sync.polarities.npy",
    "raw_ephys_data/_spikeglx_sync.times.npy",
    "raw_ephys_data/probe00/_iblqc_ephysChannels.apRMS.npy",
    "raw_ephys_data/probe00/_iblqc_ephysChannels.labels.npy",
    "raw_ephys_data/probe00/_iblqc_ephysChannels.rawSpikeRates.npy",
    "raw_ephys_data/probe00/_iblqc_ephysSpectralDensityAP.freqs.npy",
    "raw_ephys_data/probe00/_iblqc_ephysSpectralDensityAP.power.npy",
    "raw_ephys_data/probe00/_iblqc_ephysSpectralDensityLF.freqs.npy",
    "raw_ephys_data/probe00/_iblqc_ephysSpectralDensityLF.power.npy",
    "raw_ephys_data/probe00/_iblqc_ephysTimeRmsAP.rms.npy",
    "raw_ephys_data/probe00/_iblqc_ephysTimeRmsAP.timestamps.npy",
    "raw_ephys_data/probe00/_iblqc_ephysTimeRmsLF.rms.npy",
    "raw_ephys_data/probe00/_iblqc_ephysTimeRmsLF.timestamps.npy",
    "raw_ephys_data/probe00/_spikeglx_ephysData_g0_t0.imec0.lf.cbin",
    "raw_ephys_data/probe00/_spikeglx_ephysData_g0_t0.imec0.lf.ch",
    "raw_ephys_data/probe00/_spikeglx_ephysData_g0_t0.imec0.lf.meta",
    "raw_ephys_data/probe00/_spikeglx_ephysData_g0_t0.imec0.sync.npy",
    "raw_ephys_data/probe00/_spikeglx_ephysData_g0_t0.imec0.timestamps.npy",
    "raw_ephys_data/probe00/_spikeglx_ephysData_g0_t0.imec0.wiring.json",
    "raw_ephys_data/probe00/_spikeglx_sync.channels.probe00.npy",
    "raw_ephys_data/probe00/_spikeglx_sync.polarities.probe00.npy",
    "raw_ephys_data/probe00/_spikeglx_sync.times.probe00.npy",
    "raw_ephys_data/probe01/_iblqc_ephysChannels.apRMS.npy",
    "raw_ephys_data/probe01/_iblqc_ephysChannels.labels.npy",
    "raw_ephys_data/probe01/_iblqc_ephysChannels.rawSpikeRates.npy",
    "raw_ephys_data/probe01/_iblqc_ephysSpectralDensityAP.freqs.npy",
    "raw_ephys_data/probe01/_iblqc_ephysSpectralDensityAP.power.npy",
    "raw_ephys_data/probe01/_iblqc_ephysSpectralDensityLF.freqs.npy",
    "raw_ephys_data/probe01/_iblqc_ephysSpectralDensityLF.power.npy",
    "raw_ephys_data/probe01/_iblqc_ephysTimeRmsAP.rms.npy",
    "raw_ephys_data/probe01/_iblqc_ephysTimeRmsAP.timestamps.npy",
    "raw_ephys_data/probe01/_iblqc_ephysTimeRmsLF.rms.npy",
    "raw_ephys_data/probe01/_iblqc_ephysTimeRmsLF.timestamps.npy",
    "raw_ephys_data/probe01/_spikeglx_ephysData_g0_t0.imec1.lf.cbin",
    "raw_ephys_data/probe01/_spikeglx_ephysData_g0_t0.imec1.lf.ch",
    "raw_ephys_data/probe01/_spikeglx_ephysData_g0_t0.imec1.lf.meta",
    "raw_ephys_data/probe01/_spikeglx_ephysData_g0_t0.imec1.sync.npy",
    "raw_ephys_data/probe01/_spikeglx_ephysData_g0_t0.imec1.timestamps.npy",
    "raw_ephys_data/probe01/_spikeglx_ephysData_g0_t0.imec1.wiring.json",
    "raw_ephys_data/probe01/_spikeglx_sync.channels.probe01.npy",
    "raw_ephys_data/probe01/_spikeglx_sync.polarities.probe01.npy",
    "raw_ephys_data/probe01/_spikeglx_sync.times.probe01.npy",
]

# %% Get all of the datasets belonging to the eids above
all_dids = []

dsets = Dataset.objects.filter(session__in=EIDS)
columns = ['id', 'session', 'collection', 'name', 'dataset_type__name', 'default_dataset']
df_datasets_all = pd.DataFrame(dsets.values_list(*columns), columns=columns)
df_datasets_all.set_index('id', inplace=True)

# %% now prune the dataframe according to the ephys dataset relative paths
df_datasets_all['release'] = False
# No dids provided so we assume the default dataset was used for each ephys dataset.
for did, rec in tqdm.tqdm(df_datasets_all.iterrows(), total=len(df_datasets_all)):
    if f'{rec.collection}/{rec["name"]}' in datasets_relative_paths and rec['default_dataset']:
        df_datasets_all.at[did,'release'] = True
        continue

print(df_datasets_all.release.value_counts())

# %% Save the current parquet file
df_datasets = df_datasets_all.loc[df_datasets_all.release, :]
df_datasets = df_datasets.reset_index().rename(columns={'id': 'dataset_id'}).drop(columns=['release'])
df_datasets['session'] = df_datasets['session'].astype(str)
df_datasets['dataset_id'] = df_datasets['dataset_id'].astype(str)
# Save dataset IDs for release in public database
df_datasets.to_parquet(IBL_ALYX_ROOT.joinpath(f'{TAG_NAME}.pqt'))

# %% Tagging in production database
if DRY_RUN is False:
    dsets2tag = Dataset.objects.filter(id__in=df_datasets['dataset_id'])
    tag, _ = Tag.objects.get_or_create(name=TAG_NAME, protected=True, public=True)
    tag.datasets.set(dsets2tag)

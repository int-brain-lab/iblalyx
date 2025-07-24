"""
Data Release request link:
https://docs.google.com/document/d/1ziHSzUoGWHMi8YU3JtCr2Q8QTlbZUllmtxxarEY9ab4/edit?tab=t.0
"""

from pathlib import Path
import tqdm

import pandas as pd
from data.models import Dataset, Tag

PATH_IBL_ALYX = Path('/home/olivier/PycharmProjects/alyx_ferret/iblalyx')
TAG_NAME = '2025_Q3_Meijer_et_al_serotonin'
EIDS = [
    "066bf23b-17a9-4a0a-84ac-097776a2ea4a",
    "08371a0c-22d9-4f6e-ac22-c15788b39180",
    "0d04401a-2e75-4449-b699-252000ed2b76",
    "0d24afce-9d3c-449e-ac9f-577eefefbd7e",
    "126cf6e8-fc3a-4fab-aad4-75772af0aa5d",
    "2457a923-5efb-42ba-80c2-a83a5b659e28",
    "27c5e5e6-0359-4375-b53e-677aa57a73c0",
    "299bf322-e039-4444-8f04-67fde247ae5e",
    "2d93903c-d02b-4e0d-9218-681f2884598d",
    "2ec37e95-4266-45ac-8a42-a1b2d5bc8b7e",
    "30e67309-433b-4839-88ad-2bef7b33d39e",
    "31684e30-6708-4816-a8cb-c32c63ea7541",
    "37ba7dd8-714f-4c63-a7e3-548e35d8664a",
    "3fce3ab0-3833-4012-9b4f-9fbf83bb7c01",
    "401dc15a-96d1-47a9-81f5-bebdfcd2465d",
    "40b82e4e-4df4-4638-8c06-1d7ce0a58015",
    "40d3fa8b-3c47-4146-ad7a-c38dc7323e4e",
    "5244890d-9353-4cfe-a18e-23481883e3a4",
    "53771a15-33be-4a62-a779-a8c609b742e2",
    "54014195-8d46-4a06-a291-4761d1c98f10",
    "5531e71f-8ab9-4c4e-8d5b-d92da838ee16",
    "595c8ddd-a8e3-438d-9c31-62b2a07cc6cf",
    "5b7e1b65-b844-4fe8-b19e-1c3175290a63",
    "6c49b1c9-44e5-4e8a-b62f-f4b8a5960f2b",
    "731910b7-8e36-4599-be41-dddbb8c9b198",
    "73fa004c-eac3-4b34-997a-9b4268990474",
    "7ab26936-81b3-482d-a60f-538e2e325f1e",
    "7d962737-6858-4d85-9420-c83d20e3ff0a",
    "803dd5b6-248a-4811-b36b-d7070bbfa3a1",
    "865496ae-3206-4974-af44-2cff2f7023f2",
    "87287aa4-727e-48c0-8b7d-92c9f35cc257",
    "8a1808ee-44ab-4572-b7ac-921a1af98748",
    "8e1bcbcb-43a3-4e08-8bdd-295255de6bb8",
    "91987e95-33ef-4af8-a2a9-377d26a97a52",
    "91cb57bf-bf33-4fa4-9880-7c03da910ac2",
    "91e629f5-f161-44e4-8dd0-15954c602f02",
    "a3b8947c-4fb1-43ef-a8df-c51fffe78c11",
    "a456692f-5658-430f-b748-c864ace98b6c",
    "a5f5eacc-3344-4f24-a41a-a6018c1eabf1",
    "a8b5714d-2b1b-4902-9060-a6b205686682",
    "ae4a54de-43c9-4eff-8a7d-bd2d05c5f993",
    "aff57b75-b439-4537-be22-743acedc8e5d",
    "bd02e951-760a-448d-9c52-3f2990f9cf42",
    "bec8e669-cc9a-4d4d-bdb7-5e9604fe2fcc",
    "c1fbabc4-21b1-47f9-92b0-57ed9da5ec4b",
    "c211d400-04aa-479f-9544-3d30e29b8c5f",
    "c3ac69ad-851a-475c-8ba8-e44e9bd76dcb",
    "c5eff7e4-71ad-4983-96ac-a7e5affaadbb",
    "c5f0df31-60ca-42da-900d-7881fbc40c23",
    "c8f68575-e5a8-4960-a690-0149c5c4683f",
    "cce8c6b0-2695-42d3-bc35-af35b4546c65",
    "d0387aa6-b648-466a-b5a6-bb647c8acc41",
    "d0d46f77-1066-4568-a984-b2469f948aa2",
    "d45f1a4a-7f14-4f84-a85c-34b21c6d498c",
    "dfa58635-10b6-48d2-b77b-c4022ad0899a",
    "e0b73b6d-3409-441b-95ce-f22f5d22d380",
    "e3261da8-3d99-4cca-aabc-ef51ed2e5abc",
]

datasets_relative_paths = [
    "alf/_ibl_trials.table.pqt",
    "alf/_ibl_trials.laserStimulation.npy",
    "alf/_ibl_trials.laserProbability.npy",
    "alf/_ibl_leftCamera.dlc.pqt",
    "alf/leftCamera.ROIMotionEnergy.npy",
    "alf/leftROIMotionEnergy.position.npy",
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
    "raw_video_data/_iblrig_leftCamera.frameData.bin",
    "raw_video_data/_iblrig_leftCamera.raw.mp4",
]

# %%
all_dids = []

dsets = Dataset.objects.filter(session__in=EIDS)
columns = ['id', 'session', 'collection', 'name']
df_datasets_all = pd.DataFrame(dsets.values_list(*columns), columns=columns)
df_datasets_all.set_index('id', inplace=True)

# %% now prune the dataframe
df_datasets_all['release'] = False
for did, rec in tqdm.tqdm(df_datasets_all.iterrows(), total=len(df_datasets_all)):
    if f'{rec.collection}/{rec["name"]}' in datasets_relative_paths:
        df_datasets_all.at[did,'release'] = True
        continue
    if rec['name'].startswith('_ibl_trials.'):
        df_datasets_all.at[did, 'release'] = True
        continue

print(df_datasets_all.release.value_counts())

# %%
df_datasets = df_datasets_all.loc[df_datasets_all.release, :]
df_datasets = df_datasets.reset_index().rename(columns={'id': 'dataset_id'}).drop(columns=['release'])
df_datasets['session'] = df_datasets['session'].astype(str)
df_datasets['dataset_id'] = df_datasets['dataset_id'].astype(str)
# Save dataset IDs for release in public database
df_datasets.to_parquet(PATH_IBL_ALYX.joinpath('releases', f'{TAG_NAME}.pqt'))


# Tagging in production database
# tag, _ = Tag.objects.get_or_create(name="2025_Q3_Meijer_et_al", protected=True, public=True)
# tag.datasets.set(dsets2tag)

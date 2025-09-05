"""
Data Release request link:
https://docs.google.com/document/d/1ziHSzUoGWHMi8YU3JtCr2Q8QTlbZUllmtxxarEY9ab4/edit?tab=t.0

- TODO for the trials tables: do a read after write including the laser stimulations
"""

from pathlib import Path
import tqdm
import sys

import pandas as pd
from data.models import Dataset, Tag
import alyx.base

IBL_ALYX_ROOT = Path(alyx.base.__file__).parents[3].joinpath('iblalyx')
assert IBL_ALYX_ROOT.exists(), 'No IBL_ALYX_ROOT found, it is usually at the same directory level as the alyx repo'
sys.path.append(str(IBL_ALYX_ROOT.parent))

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

EIDS += [  # this is a second batch of EIDs that Guido gave me https://int-brain-lab.slack.com/archives/CEM70EXTN/p1756285788486999
"16ed8681-52f4-4be5-af8d-94c2f9cf0d0a",
"f0770fb5-80ea-476f-9ec8-c30385ba8546",
"719aa366-e467-4111-b605-d363b73a4490",
"1bcae406-0251-425a-8cc6-cabd18001439",
"cc97d121-9272-42da-8797-8afb02b10482",
"8e3e56d1-4e74-49e1-94ff-65771ac852b5",
"9974a090-c1d5-4215-8bea-558e155e5669",
"2bfc9363-e976-46bf-b4c4-a0dffff196f3",
"aa67dde1-8223-4d97-b0f9-93c9bb1be99f",
"ee1506a2-7294-4e3b-94fa-bf52d20f3730",
"f9bd58f9-7364-4e0d-89a4-ae77baa96dd2",
"19b4da41-162d-4a00-9f0b-bf2d8ab51de9",
"3c6e1d91-5e88-4580-98fe-4a546abdf5ac",
"68350c8d-ae99-43c3-96bf-0bfc234eb542",
"f3dfc69b-f7a3-457a-a4bb-67f5c34a8b94",
"7556168c-558f-46f0-9bc1-a7baa33a626b",
"a98812f6-f5ac-4022-a17b-8163f26fa780",
"1a7d8360-8692-4bfc-9866-91a86cf58a41",
"a2ebd2a4-3ff2-49e7-a22c-1661d6500dc2",
"00c4ee96-3b51-4d6a-a3cc-442b637c9de1",
"ced8cc25-a121-46ea-bd87-33cbc45fdcdb",
"8fdfe480-b016-48fa-8d57-191a07db4e8e",
"54c8eada-31d4-4c8d-bcc6-a76097b5ed17",
"30d70f60-553b-4325-ae83-a35f23271c02",
"56fc65b3-8e8b-4e65-a935-71cc75eb5e5c",
"292a0657-8d38-4980-a640-fad796cdf82b",
"766314e5-d59e-430d-8d2c-5af9ba9128d1",
"95aa2185-dbd1-467a-9069-4d295eb85e57",
"84785175-b255-4c62-b32c-783366dbbbec",
"42224df7-a48b-497f-841a-719078baac20",
"9ae2e3a7-55a8-4ded-bc91-83d47df67eaa",
"91f3d56f-c70f-4b2c-bfef-8c5c86e4a3d2",
"41ef4156-b899-4fb1-92e1-623a0834683d",
"c781f083-336f-480b-8978-85c55a88c828",
"63fdf495-0426-4d01-a6a5-bac7bade8b2a",
"4a4be3d6-6718-4196-8248-0dd3c81bfb0f",
"effa5aff-b7ed-45fb-9587-c78fbe533b60",
"7b7e9b31-42eb-4498-aff1-dce523673779",
"9b4dd97a-e5c7-404c-919b-89391e618e1e",
"52945e01-92a8-4c5c-8367-2af6b0aa7a19",
"935f304e-8091-402f-ad63-e330b65650ea",
"89ca5a44-8be9-4f04-9788-e55527e5adfa",
"ef60a4cf-aa4d-4919-80b7-f6d5943ad387",
"a020dbcd-0f4e-4485-ac58-cee7d279939a",
"30b979c3-ff45-4c4e-a01c-c5a558a92d7f",
"c3bb0ccf-8274-4158-8efe-a3c7b3e0f8b6",
"aebbf926-f02a-4aa8-a190-84fb367ce5fb",
"0f2b5f9d-aff2-4adf-92ca-e0f7ba43f9ae",
"d0f1646a-5ba6-4612-bb5d-d8422e9dc90e",
"aad576e1-7a12-45ad-83f6-0e91492fc29f",
"0a82ffaf-cbcf-4b54-b71d-866d3b2544e8",
"a76515bd-2527-4620-b1d1-a23d04cdd900",
"039ced91-2ba9-4956-a97a-162fdd84b8dc",
"e2db9a5f-a99e-4b63-bff0-729b66d213e8",
"21363f28-9683-451f-8f80-3f9aee005a6c",
"7c995107-fc32-4a3c-b01b-41ec39232c9d",
"e9db3233-81c8-4b33-94ea-d209aa741403",
"18fb3a09-18e3-4d4b-8eff-f12d1509a553",
"9ca6c0d6-e598-4d35-8cc4-33b5fd7a8d55",
"9bcf7d7a-c293-40f0-8b92-f85d82f33888",
"b395f1fa-a89c-41af-96fe-ecf6525ff529",
"d85ad1a8-d17f-4a9f-b440-303e13e0691f",
"672ae227-b10c-4c42-9ade-72e4a05ce85c",
"d70d2e4c-1174-4b3c-a7e7-61af9feaa05f",
"b8ee0782-9bca-4b6e-92d9-8ecc577b1eba",
"9ad46606-b3c7-4e71-8fa6-cb9decdad09f",
"5f06cef5-4aff-4719-9a95-85a0a0f75ad9",
"1d3b694b-f781-40b3-aacc-09729648fdfc",
"92147c4f-c792-4afd-8504-8dc47d6f2115",
"d1558ba2-c411-4282-807f-cc5f3e90335b",
"ee39969e-bdb6-44b7-a742-d06d8e80cbbe",
"e3ab0de3-4f9c-48fb-93f2-65f67a44b88c",
"998b3667-bd20-4d4d-b840-953b868ea90b",
"f9e70337-cf10-4073-8a55-06a8168d6587",
"85b22c83-4688-4d02-9f52-99f7cb869a2b",
"bc05ec57-9500-48aa-9825-da7b3f4694f4",
"26504a8e-ee06-435d-91c1-e1b541a2df5f",
"67fee31a-215b-4836-8044-973c18fce4c1",
"ab87fff3-45df-4937-9536-9495c20562d3",
"386b0b66-bb44-48b1-8b34-9e1a378fd7c6",
"880a8764-e8e2-4ed8-a648-b77b387ba9aa",
"13fe7d4b-009c-4233-ac93-2da6c0dd78f4",
"04d6ccf0-16f0-43bf-a85f-2f779454e512",
"8f45f39b-067e-4fab-a920-4fe89005cf2f",
"63dfb36c-65d3-46f6-a5c9-23c3c644763f",
"c2111f95-094e-435f-a2a6-4ff8e53338a2",
"066fbb45-4af6-45c9-a029-ad8903090f13",
"61ad4683-739f-4463-bf03-3ac56b719f47",
"6c064581-790e-4150-8b1b-4667f18e3dcf",
"3e752e1d-54d9-411d-b238-b4f975eeb660",
"b1916e32-8d9f-4fdd-8c7a-15168b6c0e2b",
"c1e0a653-9ce5-414f-a3d7-2584628f018a",
"7c3d5dd2-32fe-4582-8044-ef61b433240e",
"0bc5a917-b45b-4612-9599-18047e13d547",
"59bd6f24-e3dd-47f1-a8cf-2f4bd2c1dacb",
"29b02d0f-5092-4fa5-a433-0d2ea3eafca8",
"0281772e-b4c1-4680-81af-f7a56f1870b9",
"b6b60654-e236-4559-be65-188ab7d36a2b",
"9f389f8c-7606-4490-ac2f-37fb4bf92dcf",
"6fcbb3b5-7315-4471-80b8-16c2e8387b32",
"3d2499db-1947-4c3b-917c-80bc2a1bac3a",
"31ee2d4c-f12a-473d-aad5-24ef5acbcfd0",
"b18b8d56-e561-4f9e-b7d1-c7c434a4907e",
"e2911a64-a061-4058-a5dc-4221ed33d463",
"bd98559c-d3fb-4fef-acfe-aabdb2862549",
"fd96c3d3-fd0c-4b49-8405-e4d53e8319d8",
"e7872b06-8dcd-4dc0-9fba-076aeddbe7e1",
"99973500-3673-4127-9a70-528a333cbec9",
"b82d0e5b-c4fa-4477-b195-fd29bf20a595",
"d24f3163-ac79-40c4-b653-fc1c5e13c89b",
"eef2300a-5fcd-4c9e-bfee-ff5ab1b419d0",
"d3bccda9-14fa-406f-b424-9d7e18159dce",
"3dbee76c-38bc-4afa-b58d-973a5158fe6f",
"1aba35fd-9d82-4d05-a275-e33347fbcc70",
"4e24001d-de3e-4737-b20c-97a4b252c8cd",
"2852aac3-0b80-41fd-9d22-bfb696787c4d",
"771ad126-7537-4caa-b999-6b843bf622a7",
"68368842-9543-45c8-8d75-b66eef40837f",
"1e988a23-539e-4cfc-8d92-9756f3b2f360",
"3a06738c-bd9f-4f5d-8309-85d2f617dd5b",
"caf38d85-1ef8-4536-935a-f297756e2e43",
"24f4d18e-67f8-47f6-a519-2fd99a0134cd",
"48773806-6f1c-4821-8cde-7715d1fb7422",
"f3070b25-e9a7-4374-8867-7c13e791e26f",
"c01f7f5c-dabb-4e97-a9b7-120075c9dba0",
]

datasets_relative_paths = [
    "alf/_ibl_trials.table.pqt",
    "alf/_ibl_trials.laserStimulation.npy",
    "alf/_ibl_trials.laserProbability.npy",
    "alf/_ibl_wheel.timestamps.npy",
    "alf/'_ibl_wheel.position.npy",
    'alf/_ibl_wheelMoves.intervals.npy',
    'alf/_ibl_wheelMoves.peakAmplitude.npy',
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

DRY_RUN = True
TAG_NAME = '2025_Q3_Meijer_et_al_serotonin'
all_dids = []

dsets = Dataset.objects.filter(session__in=EIDS)
columns = ['id', 'session', 'collection', 'name', 'dataset_type__name']
df_datasets_all = pd.DataFrame(dsets.values_list(*columns), columns=columns)
df_datasets_all.set_index('id', inplace=True)

# %% now prune the dataframe according to the dataset relative paths provided by Guido
df_datasets_all['release'] = False
for did, rec in tqdm.tqdm(df_datasets_all.iterrows(), total=len(df_datasets_all)):
    if f'{rec.collection}/{rec["name"]}' in datasets_relative_paths:
        df_datasets_all.at[did,'release'] = True
        continue
    if rec['name'].startswith('_ibl_trials.'):
        df_datasets_all.at[did, 'release'] = True
        continue

print(df_datasets_all.release.value_counts())

# %% we add the video datasets according to the QC provided

import iblalyx.releases.utils
dsets_video = iblalyx.releases.utils.get_video_datasets_for_ephys_sessions(EIDS)
dsets_video = [did for did in dsets_video.values_list('id', flat=True)]

df_datasets_all.loc[dsets_video, 'release'] = True
print(df_datasets_all.release.value_counts())

# %% Save the current parquet file
df_datasets = df_datasets_all.loc[df_datasets_all.release, :]
df_datasets = df_datasets.reset_index().rename(columns={'id': 'dataset_id'}).drop(columns=['release'])
df_datasets['session'] = df_datasets['session'].astype(str)
df_datasets['dataset_id'] = df_datasets['dataset_id'].astype(str)
# Save dataset IDs for release in public database
df_datasets.to_parquet(IBL_ALYX_ROOT.joinpath('releases', f'{TAG_NAME}.pqt'))

# %% Tagging in production database
if DRY_RUN is False:
    dsets2tag = Dataset.objects.filter(id__in=df_datasets['dataset_id'])
    tag, _ = Tag.objects.get_or_create(name="2025_Q3_Meijer_et_al", protected=True, public=True)
    tag.datasets.set(dsets2tag)

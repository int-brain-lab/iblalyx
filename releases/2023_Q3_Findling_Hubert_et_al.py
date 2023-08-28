import pandas as pd
from data.models import Tag, Dataset

wfield_dataset_types = [
 'imaging.times.npy',
 'imagingLightSource.properties.htsv',
 'imaging.imagingLightSource.npy',
 'widefieldLandmarks.dorsalCortex.json',
 'widefieldSVT.haemoCorrected.npy',
 'widefieldU.images.npy',
 '_ibl_trials.table.pqt',
 '_ibl_trials.goCueTrigger_times.npy',
 '_ibl_trials.quiescencePeriod.npy',
 '_ibl_trials.laserProbability.npy',
 '_ibl_trials.stimOff_times.npy'
 ]

wfield_eids = pd.read_csv('2023_Q3_Findling_Hubert_et_al_wfield_sessions.csv', index_col=0)['session_id']
wfield_datasets = Dataset.objects.filter(session__in=wfield_eids, name__in=wfield_dataset_types)

pupil_eids = pd.read_csv('2023_Q3_Findling_Hubert_et_al_pupil_sessions.csv', index_col=0)['session_id']
pupil_datasets = Dataset.objects.filter(session__in=pupil_eids, name='_ibl_leftCamera.lightningPose.pqt')

datasets = wfield_datasets | pupil_datasets

tag, _ = Tag.objects.get_or_create(name="2023_Q3_Findling_Hubert_et_al", protected=True, public=True)
tag.datasets.set(datasets)

# Save dataset IDs for release in public database
dset_ids = [str(did) for did in datasets.values_list('pk', flat=True)]
df = pd.DataFrame(dset_ids, columns=['dataset_id'])
df.to_parquet('2023_Q3_Findling_Hubert_et_al_datasets.pqt')

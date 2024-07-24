import pandas as pd
from data.models import Dataset, Tag

sessions = pd.read_csv('./2024_Q3_Pan_Vazquez_et_al_sessions.csv')

photometry_datasets = [
 'photometry.signal.pqt',
 'photometryROI.locations.pqt',
 '_ibl_trials.table.pqt',
 '_ibl_trials.stimOnTrigger_times.npy',
 '_ibl_trials.quiescencePeriod.npy',
 '_ibl_trials.goCueTrigger_times.npy',
 '_ibl_wheel.position.npy',
 '_ibl_wheel.timestamps.npy',
 '_ibl_wheelMoves.intervals.npy',
 '_ibl_wheelMoves.peakAmplitude.npy',
 '_iblrig_stimPositionScreen.raw.csv']

training_datasets = [
 '_ibl_trials.goCueTrigger_times.npy',
 '_ibl_trials.laserIntervals.npy',
 '_ibl_trials.laserStimulation.npy',
 '_ibl_trials.quiescencePeriod.npy',
 '_ibl_trials.stimOnTrigger_times.npy',
 '_ibl_trials.table.pqt',
 '_ibl_wheel.position.npy',
 '_ibl_wheel.timestamps.npy',
 '_ibl_wheelMoves.intervals.npy',
 '_ibl_wheelMoves.peakAmplitude.npy',
 '_iblrig_stimPositionScreen.raw.csv']

photometry_sessions = sessions[sessions['has_photometry']].eid.values
ph_dsets = Dataset.objects.filter(session__in=photometry_sessions, name__in=photometry_datasets)
training_sessions = sessions[~sessions['has_photometry']].eid.values
tr_dsets = Dataset.objects.filter(session__in=training_sessions, name__in=training_datasets)
dsets = ph_dsets | tr_dsets

# Tagging in production database
tag, _ = Tag.objects.get_or_create(name="2024_Q3_Pan_Vazquez_et_al", protected=True, public=True)
tag.datasets.set(dsets)

# Save dataset IDs for release in public database
dset_ids = [str(eid) for eid in dsets.values_list('pk', flat=True)]
df = pd.DataFrame(dset_ids, columns=['dataset_id'])
df.to_parquet('./2024_Q4_Pan_Vazquez_et_al_datasets.pqt')


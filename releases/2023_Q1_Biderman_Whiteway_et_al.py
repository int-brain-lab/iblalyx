import pandas as pd
from data.models import Tag, Dataset

# Load session and probe info
df = pd.read_csv('2023_Q1_Biderman_Whiteway_et_al_sessions.csv', index_col=0)

# Record the datasets loaded by load_good_units
from one.api import ONE
from brainwidemap.bwm_loading import load_good_units
one = ONE(base_url='https://openalyx.internationalbrainlab.org')
one.record_loaded = True
for pid in df['pid']:
    load_good_units(one, pid)
spikes, filename = one.save_loaded_ids(clear_list=False)
spikes = Dataset.objects.filter(id__in=spikes)

# Left video, timestamps and DLC
vid_left = Dataset.objects.filter(session_id__in=df[df['left_video'] == True]['eid'].unique(),
                                  name__in=['_ibl_leftCamera.dlc.pqt',
                                            '_ibl_leftCamera.times.npy',
                                            '_iblrig_leftCamera.raw.mp4'],
                                  default_dataset=True)

# Left and right video, timestamps and DLC
vid_right = Dataset.objects.filter(session_id__in=df[df['right_video'] == True]['eid'].unique(),
                                   name__in=['_ibl_rightCamera.dlc.pqt',
                                             '_ibl_rightCamera.times.npy',
                                             '_iblrig_rightCamera.raw.mp4'],
                                  default_dataset=True)
# Trials tables
trials = Dataset.objects.filter(session_id__in=df['eid'].unique(),
                                name='_ibl_trials.table.pqt', default_dataset=True)


datasets = spikes | vid_left | vid_right | trials
tag, _ = Tag.objects.get_or_create(name="2023_Q1_Biderman_Whiteway_et_al", protected=True, public=True)
tag.datasets.set(datasets)


dset_ids = [str(d.id) for d in datasets]
df = pd.DataFrame(dset_ids, columns=['dataset_id'])
df.to_parquet('./2023_Q1_Biderman_Whiteway_et_al_datasets.pqt')
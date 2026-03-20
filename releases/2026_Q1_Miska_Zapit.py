"""Save the dataset and session IDs for Nate's RT Heatmap on Brain Atlas figure.

To run, ensure the zapit files are in the python path and set SAVE_FIGURES in zapit/config.py to False.
"""
import metadata_zapit
import pandas as pd
from config import ALYX_BASE_URL
from one.api import ONE

one = ONE(base_url=ALYX_BASE_URL)
one.record_loaded = True
# Set the tables directory to the current directory
# This is where the loaded IDs will be saved when calling `one.save_loaded_ids()`
one._tables_dir = one._tables_dir.__class__(__file__).parent

import zapit_analysis  # Nate's analysis script

# Save the dataset IDs to file
dataset_uuids, filename = one.save_loaded_ids(clear_list=False)
filename = filename.replace(filename.with_stem('2026_Q1_Miska_Zapit_datasets'))
print(filename)
print(pd.read_csv(filename), end='\n\n')

# Save the session IDs
session_uuids, filename = one.save_loaded_ids(sessions_only=True)
filename = filename.replace(filename.with_stem('2026_Q1_Miska_Zapit_sessions'))
print(filename)
print(pd.read_csv(filename))

eids = [x['EID'] for x in metadata_zapit.sessions]
unused = set(eids) - set(map(str, session_uuids))
print(f'{len(unused)} sessions were not used in the figure')

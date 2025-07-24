"""

Other data repository: https://osf.io/fap2s/
Nature Neuroscience | Volume 28 | July 2025 | 1519–1532 1519 nature neuroscience
https://doi.org/10.1038/s41593-025-01965-8

https://osf.io/fap2s/wiki/home/

- for the trials tables: do a read after write before

"""

# %%
from pathlib import Path
import pandas as pd  # uv pip install openpyxl
TAG_NAME = '2025_Q3_Noel_et_al_Autism'

project_name = 'angelaki_mouseASD'

for xls_file in Path('/home/olivier/scratch/autism').glob('*.xlsx'):
    df = pd.read_excel(xls_file)
    break

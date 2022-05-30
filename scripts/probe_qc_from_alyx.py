from pathlib import Path

from django.db.models import Sum
from data.models import DatasetType, Dataset
from actions.models import Session
from experiments.models import ProbeInsertion

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

# %% get size of BWM datasets
ses = Session.objects.filter(project__name__icontains='ibl_neuropixel_brainwide_01', qc__lt=50,
                             extended_qc__behavior=1, json__IS_MOCK=False)

dsets = Dataset.objects.filter(session__in=ses, name__endswith='ap.cbin')
dsets.aggregate(siz=Sum('file_size'))['siz'] / 1024 ** 4



# %% Get Insertin QCs to look at whitening
ins = ProbeInsertion.objects.filter(
    session__project__name__icontains='ibl_neuropixel_brainwide_01',
    session__qc__lt=50,
    session__json__IS_MOCK=False,
).exclude(
    json__qc='CRITICAL'
)

ins.count()



cnames = ['id', 'session', 'session__start_time', 'session__number', 'session__subject__nickname',
          'json__n_units', 'json__drift_rms_um', 'json__firing_rate_median', 'json__whitening_matrix_conditioning'
          ]
ins_qc = pd.DataFrame(ins.values_list(*cnames), columns=cnames)
sns.set_theme()
white = ins_qc['json__whitening_matrix_conditioning'].values
fr = ins_qc['json__firing_rate_median'].values



sel = np.logical_and(~np.isnan(white), ~np.isnan(fr))
plt.plot(np.log(white[sel]), fr[sel], '.')

sns.histplot(np.minimum(white[sel], 99),binrange=[0, 100])
plt.xlabel('Condition number')


ins_qc['id'][ins_qc['json__whitening_matrix_conditioning'] > 50]
ins_qc['json__whitening_matrix_conditioning'][ins_qc['json__whitening_matrix_conditioning'] > 50]
# 31     5d9a5895-0f28-416c-a6d6-3e781658ea57
# 37     f3988aec-b8c7-47d9-b1a0-5334eed7f92c
# 143    04690e35-ab38-41db-982c-50cbdf8d0dd1
# 155    45c49ba2-a113-4446-9c6d-9b049c1f9f74
# 363    c536b0a9-b710-43c3-ba2c-5a0cccf98ec2
# 387    bdd57835-38b0-46ff-acda-45e14b8d6293
# 396    5df09eb0-47cb-4554-bc7a-2e91995b7b07
# 399    6c897616-f03c-442c-acd8-109fd97ba463
# 406    316a733a-5358-4d1d-9f7f-179ba3c90adf
# 432    e10a7a75-4740-41d1-82bb-7696ed14c442
# 437    154b7adb-c947-4b65-a0ff-c0bb707e1564
# 438    78cb9d69-a49a-480f-b182-7f7985a5b1dd
# 585    f9656eee-141c-453d-a016-4aba68f674dc
# 596    57acb665-9a6d-4240-ab39-41013bdf7098
# 681    71a92c54-69f0-488b-ae2a-cb6c1524233c
# 693    4762e8ed-4d94-4fd7-9522-e927f5ffca74


# 31        70.04
# 37        97.02
# 143     4886.53
# 155     8891.92
# 363     6331.49
# 387    12804.08
# 396     6951.36
# 399     3070.04
# 406       96.83
# 432     5416.90
# 437    13949.34
# 438     3785.07
# 585     3379.77
# 596     3460.45
# 681     2121.92
# 693     3082.65


# %% Get the number of failing passive sessions
from jobs.models import Task
from django.db.models import Count
from actions.models import Session
ses = Session.objects.filter(project__name__icontains='ibl_neuropixel_brainwide_01', qc__lt=50,
                             extended_qc__behavior=1, json__IS_MOCK=False)

tasks = Task.objects.filter(name__icontains='passive', session__in=ses)
res = tasks.values('status').annotate(count=Count('status'))


# Out[3]: <QuerySet [{'status': 40, 'count': 31}, {'status': 45, 'count': 3}, {'status': 50, 'count': 8}, {'status': 55, 'count': 28}, {'status': 60, 'count': 329}]>

STATUS_DATA_SOURCES = [
    (20, 'Waiting',),
    (25, 'Held',),
    (30, 'Started',),
    (40, 'Errored',),
    (45, 'Abandoned',),
    (50, 'Empty'),
    (55, 'Incomplete'),
    (60, 'Complete',),
]


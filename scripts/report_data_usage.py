from pathlib import Path

from django.db.models import Sum
from data.models import DatasetType, Dataset

import pandas as pd

dtypes = DatasetType.objects.all().annotate(size=Sum('dataset__file_size'))

df_types = pd.DataFrame.from_records(dtypes.values())
df_types['size'] = df_types['size'] / 1024 ** 3
df_types.to_csv(Path.home().joinpath('dataset_types.csv'))

tot = dtypes.aggregate(siz=Sum('size'))['siz'] / 1024 ** 4

s = {
    'ephys': dtypes.filter(name__istartswith='ephysData').aggregate(siz=Sum('size'))['siz'] / 1024 ** 4,
    'video': dtypes.filter(name__icontains='camera').aggregate(siz=Sum('size'))['siz'] / 1024 ** 4,
    'extracted': Dataset.objects.filter(collection__startswith='alf').aggregate(Sum('file_size'))['file_size__sum'] / 1024 ** 4,
}

s['experimental'] = tot - s['ephys'] - s['video'] - s['extracted']



import seaborn as sns
sns.set_theme()



data = s.values()
labels = s.keys()
explode = [0.1 for d in data]
#define Seaborn color palette to use
colors = sns.color_palette('pastel')[0:5]
import matplotlib.pyplot as plt
#create pie chart
# plt.pie(data, labels = labels, colors = colors, autopct='%.0f%%')

plt.pie(data, colors = colors, autopct='%.0f%%', explode=explode)
plt.legend(labels=labels)

plt.show()
total = dtypes.aggregate(siz=Sum('size'))['siz'] / 1024 ** 4

ephys = dtypes.filter(name__istartswith='ephysData').aggregate(siz=Sum('size'))['siz'] / 1024 ** 4

vid = dtypes.filter(name__icontains='camera').aggregate(siz=Sum('size'))['siz'] / 1024 ** 4


trains = dtypes.filter(name__icontains='spikes.').aggregate(siz=Sum('size'))['siz'] / 1024 ** 3
trains += dtypes.filter(name__icontains='clusters.').aggregate(siz=Sum('size'))['siz'] / 1024 ** 3
trains += dtypes.filter(name__icontains='channels.').aggregate(siz=Sum('size'))['siz'] / 1024 ** 3


print(f'total {total} Tb')
print(f'ephys {ephys} Tb')
print(f'vid {vid} Tb')
print(f'trains {trains} Tb')

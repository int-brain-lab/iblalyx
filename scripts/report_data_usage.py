from pathlib import Path

from django.db.models import Sum
from data.models import DatasetType

import pandas as pd

dtypes = DatasetType.objects.all().annotate(size=Sum('dataset__file_size'))

df_types = pd.DataFrame.from_records(dtypes.values())
df_types['size'] = df_types['size'] / 1024 ** 3
df_types.to_csv(Path.home().joinpath('dataset_types.csv'))


dtypes.aggregate(siz=Sum('size'))['siz'] / 1024 ** 3

dtypes.filter(name__istartswith='ephysData').aggregate(siz=Sum('size'))['siz'] / 1024 ** 3

dtypes.filter(name__icontains='camera').aggregate(siz=Sum('size'))['siz'] / 1024 ** 3

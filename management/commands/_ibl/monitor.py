from datetime import datetime
import json
import pandas as pd
from pathlib import Path

from jobs.models import Task
from experiments.models import ProbeInsertion
from data.models import Dataset
from django.db.models import Count, Q, OuterRef, Exists, Subquery
from django.db.models.functions import Cast
from django.db.models import TextField


def monitor_dlc():
    # status - 20: Waiting / 25: Held / 30: Started / 40: Errored / 45: Abandoned / 50: Empty / 55: Incomplete / 60: Complete
    DCHOICES = {c[0]:c[1] for c in Task.status.field.choices}
    count = Task.objects.filter(name='EphysDLC').values('status').annotate(n=Count('status'))

    d = {DCHOICES[c['status']]:c['n'] for c in count}
    t = datetime.now()
    print(t, json.dumps(d))


def monitor_spikesorting():
    """
    Save pqt table with spikesorting status every day
    :return:
    """
    save_path = '/home/mayo/task_reports/spikesorting'

    df = pd.DataFrame(columns=['pid', 'eid', 'probe_name', 'lab', 'subject', 'date', 'number', 'project', 'raw_data', 'ks',
                               'pyks', 'version', 'serial_no', 'probe_type'])

    qs = ProbeInsertion.objects.all().prefetch_related('session')
    # Annotate with some useful info
    qs = qs.annotate(raw=Exists(Dataset.objects.filter(probe_insertion=OuterRef('pk'), name__endswith='ap.cbin')))
    qs = qs.annotate(ks=Exists(Dataset.objects.filter(~Q(collection__icontains='pykilosort'), probe_insertion=OuterRef('pk'),
                                                      name='spikes.times.npy')))
    qs = qs.annotate(pyks=Exists(Dataset.objects.filter(collection__icontains=f'pykilosort', probe_insertion=OuterRef('pk'),
                                                        name='spikes.times.npy')))
    qs = qs.annotate(version=Subquery(Task.objects.filter(name='SpikeSorting', session=OuterRef('session')).values('version')))

    # Write into column of pandas dataframe
    from django.db.models import FloatField
    df['pid'] = list(qs.values_list(Cast('id', output_field=TextField()), flat=True))
    df['eid'] = list(qs.values_list(Cast('session', output_field=TextField()), flat=True))
    df['probe_name'] = list(qs.values_list('name', flat=True))
    df['lab'] = list(qs.values_list('session__lab__name', flat=True))
    df['subject'] = list(qs.values_list('session__subject__nickname', flat=True))
    df['date'] = list(qs.values_list('session__start_time', flat=True))
    df['number'] = list(qs.values_list('session__number', flat=True))
    df['project'] = list(qs.values_list('session__project__name', flat=True))
    df['raw_data'] = list(qs.values_list('raw', flat=True))
    df['ks'] = list(qs.values_list('ks', flat=True))
    df['pyks'] = list(qs.values_list('pyks', flat=True))
    df['version'] = list(qs.values_list('version', flat=True))
    df['serial_no'] = list(qs.values_list('serial', flat=True))
    df['probe_type'] = list(qs.values_list('model__name', flat=True))

    # Save to parquet table
    str(datetime.now().date())
    df.to_parquet(Path(save_path).joinpath(str(datetime.now().date()) + '_spikesorting.pqt'))


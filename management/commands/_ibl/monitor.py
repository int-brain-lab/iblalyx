from datetime import datetime
import json
import pandas as pd
from pathlib import Path

from jobs.models import Task
from experiments.models import ProbeInsertion
from data.models import Dataset
from django.db.models import Count, Q, OuterRef, Exists, Subquery


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
    save_path = '/home/ubuntu/task_reports/spikesorting'

    qs = ProbeInsertion.objects.all().prefetch_related('session')
    # Annotate with some useful info
    qs = qs.annotate(raw=Exists(Dataset.objects.filter(probe_insertion=OuterRef('pk'), name__endswith='ap.cbin')))
    qs = qs.annotate(ks=Exists(Dataset.objects.filter(~Q(collection__icontains='pykilosort'), probe_insertion=OuterRef('pk'),
                                                      name='spikes.times.npy')))
    qs = qs.annotate(pyks=Exists(Dataset.objects.filter(collection__icontains=f'pykilosort', probe_insertion=OuterRef('pk'),
                                                        name='spikes.times.npy')))
    qs = qs.annotate(version=Subquery(Task.objects.filter(name='SpikeSorting', session=OuterRef('session')).values('version')))

    df = pd.DataFrame.from_records(
        qs.values_list('id', 'session', 'name', 'session__lab__name', 'session__subject__nickname', 'session__start_time',
                       'session__number', 'session__project__name', 'raw', 'ks', 'pyks', 'version', 'serial', 'model__name'),
                       columns=['pid', 'eid', 'probe_name', 'lab', 'subject', 'date', 'number', 'project', 'raw_data', 'ks',
                       'pyks', 'version', 'serial_no', 'probe_type'])

    df['pid'] = df['pid'].astype(str)
    df['eid'] = df['eid'].astype(str)

    # Save to parquet table
    df.to_parquet(Path(save_path).joinpath(str(datetime.now().date()) + '_spikesorting.pqt'))


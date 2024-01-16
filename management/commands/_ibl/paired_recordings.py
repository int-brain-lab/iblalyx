from pathlib import Path
import numpy as np
import pandas as pd
import tqdm

from django.db.models import Max, F
from actions.models import Session
from experiments.models import TrajectoryEstimate, BrainRegion

from iblatlas.atlas import BrainRegions


def compute_paired_experiments():
    regions = BrainRegions()

    # gets the latest and greatest insertion for each probe
    trajs = TrajectoryEstimate.objects.order_by('probe_insertion', '-provenance').distinct('probe_insertion')
    # here we can eventually filter by histology level
    sessions = trajs.values_list('probe_insertion__session', flat=True).distinct()

    df = []
    for eid in tqdm.tqdm(sessions):
        trs = trajs.filter(probe_insertion__session__pk=eid).values_list('id', flat=True)
        aids = np.array(BrainRegion.objects.filter(channels__trajectory_estimate__in=trs).distinct().values_list('id', flat=True)).astype(np.int32)
        i, j = np.meshgrid(aids, aids)
        ti = np.triu_indices(aids.size, k=1, m=None)
        df.append(pd.DataFrame({'aida': i[ti], 'aidb': j[ti], 'eid': str(eid)}))

    paired_experiments = pd.concat(df)

    baid = np.unique(np.r_[paired_experiments['aidb'].unique(), paired_experiments['aida'].unique()])
    pregions = pd.DataFrame({'aid': baid, 'acronym': regions.id2acronym(baid), 'cosmos': regions.remap(baid, source_map='Allen', target_map='Cosmos')})
    plinks = paired_experiments.groupby(['aida', 'aidb']).aggregate(n_experiments=pd.NamedAgg(column='eid', aggfunc='nunique')).reset_index()

    paired_experiments.to_parquet(Path.home().joinpath('scratch', 'paired_experiments.pqt'))

from pathlib import Path
import logging
import tqdm
import tempfile

import numpy as np
import pandas as pd
import boto3

from experiments.models import TrajectoryEstimate, BrainRegion
from iblatlas.atlas import BrainRegions

logger = logging.getLogger('data.transfers')
logger.setLevel(20)


def compute_upload_paired_experiments_to_s3(profile='ucl', region='eu-west-2', bucket='alyx-uploaded'):
    """
    This function computes the paired experiments dataframe and uploads it to s3 for public access
    :return:
    """
    logger.info('Computing paired experiments dataframe')
    paired_experiments, _, _ = compute_paired_experiments()
    logger.info(f'Uploading the cache to s3://{bucket}/uploaded/paired_experiments.pqt')
    session = boto3.Session(profile_name=profile, region_name=region)
    s3 = session.client('s3')
    path_aws = Path("uploaded/paired_experiments.pqt").as_posix()
    with tempfile.TemporaryDirectory() as td:
        file_pqt_paired_experiments = Path(td).joinpath('paired_experiments.pqt')
        paired_experiments.to_parquet(file_pqt_paired_experiments)
        s3.upload_file(str(file_pqt_paired_experiments), bucket, path_aws)


def compute_paired_experiments():
    """
    This function computes the paired experiments cache from the current alyx database
    :return: 3 dataframes:
    - paired_experiments: a dataframe with all the pairwise combinations of brain regions that have been recorded
    together in the same session. Columns aida, aidb, eid
    - pregions: a dataframe with the brain region information. Columns aid, acronym, cosmos
    - plinks: a dataframe with the number of experiments that have been recorded for each pair of brain regions.
    Columns aida, aidb, n_experiments
    """
    regions = BrainRegions()

    # gets the latest and greatest insertion for each probe
    trajs = TrajectoryEstimate.objects.filter(probe_insertion__session__isnull=False)
    trajs = trajs.order_by('probe_insertion', '-provenance').distinct('probe_insertion')
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
    return paired_experiments, pregions, plinks

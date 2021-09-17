"""
does the synchronisation of IBL patcher files DMZ
"""

import logging

from django.db.models import Count, Q, F

from data.models import FileRecord, Dataset
from jobs.models import Task
import data.transfers as transfers

logger = logging.getLogger('data.transfers')
logger.setLevel(20)


def ftp_delete_local():
    # get the datasets that have one file record on the DMZ
    dsets = FileRecord.objects.filter(data_repository__name='ibl_patcher'
                                      ).values_list('dataset', flat=True).distinct()
    dsets = Dataset.objects.filter(pk__in=dsets)

    frs = FileRecord.objects.filter(dataset__in=dsets, data_repository__globus_is_personal=False, exists=True)
    dsets_2del = Dataset.objects.filter(pk__in=frs.values_list('dataset', flat=True))

    transfers.globus_delete_local_datasets(dsets_2del, dry=False)


def held_status_reset():
    # looks for held statuses task if parents are all complete or waiting, in which case reset to waiting
    t = Task.objects.filter(status=25)
    t = t.annotate(n_parents=Count('parents'), n_parents_ok=Count('parents', filter=Q(status=60) | Q(status=20)))
    t = t.annotate(n_parents_ko=F('n_parents') - F('n_parents_ok'))
    t_reset = t.filter(n_parents_ko=0)
    logger.info(f'reset {t_reset.count()} held tasks to waiting with parents')
    t_reset.update(status=20)

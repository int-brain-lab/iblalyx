"""
does the synchronisation of IBL patcher files DMZ
"""

import logging

from data.models import FileRecord, Dataset
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


def ftp_upload():
    dsets = FileRecord.objects.filter(data_repository__name='ibl_patcher'
                                      ).values_list('dataset', flat=True).distinct()
    frs = FileRecord.objects.filter(dataset__in=dsets, data_repository__globus_is_personal=False, exists=False)
    gc = transfers.globus_transfer_client()
    for lab in list(frs.values_list("dataset__session__lab__name", flat=True).distinct()):
        transfers.bulk_sync(lab=lab)
    frs = FileRecord.objects.filter(dataset__in=dsets, data_repository__globus_is_personal=False, exists=False)
    transfers._globus_transfer_filerecords(frs, dry=False, gc=gc)

    transfers.bulk_sync()
import datetime
import logging
from pathlib import Path
from django.db.models import Count, Q
import globus_sdk

import one.alf.files as af
from data.models import Dataset, FileRecord, DataRepository

from data.transfers import _filename_from_file_record, globus_transfer_client

_logger = logging.getLogger()


def remove_old_datasets_local_and_server_missing():
    """
    The sync tasks label file records that can't be found anywhere as local_missing in the json file
    If after 30 days of daily attemps to upload them none can be found, this removes the datasets from the database
    :return:
    """
    cut_off_date = datetime.datetime.now() - datetime.timedelta(days=30)
    dsets = Dataset.objects.filter(file_records__json__local_missing=True, session__start_time__date__lt=cut_off_date)
    for dset in dsets:
        _logger.warning(f"deleting {dset.session},  {dset.collection}, {dset.name}")
    dsets.delete()


def remove_sessions_local_servers(labname, archive_date=None, gc=None, dry_run=False, nsessions=100):
    """
    remove_sessions_local_servers('angelakilab', archive_date='2020-06-01', nsessions=100)
    :param labname: example: 'angelakilab'
    :param archive_date: example: '2020-06-01'
    :param gc: globus transfer client
    :param nsessions: number of sessions to handle max (100)
    :param dry_run: (False) if True, just lists number of sessions to handle and returns
    :return:
    """

    server_repository = DataRepository.objects.get(lab__name=labname, globus_is_personal=True)
    dc = globus_sdk.DeleteData(gc, server_repository.globus_endpoint_id,
                               label=f'alyx archive {labname}', recursive=True)

    frs = FileRecord.objects.filter(data_repository=server_repository,
                                    dataset__session__start_time__lt=archive_date,
                                    dataset__session__procedures__name='Behavior training/tasks')
    sessions = frs.values_list('dataset__session', flat=True).distinct()
    _logger.warning(f"{labname}, before {archive_date}, {len(sessions)} sessions to archive")

    if dry_run == True:
        return

    if len(sessions) == 0:
        _logger.warning(f"no session to delete, return !")
        return

    gc = gc or globus_transfer_client()
    eids = []

    i = 0
    for ses in sessions:
        if (i % 100) == 0:
            print(i)
        if i > (nsessions - 1):
            break
        nsdsc = Count('file_records', Q(file_records__data_repository__globus_is_personal='False',
                                        file_records__exists=True))
        dsets = Dataset.objects.filter(session=ses).annotate(nsdsc=nsdsc)
        if len(dsets.filter(nsdsc=0)) == 0:
            i += 1
            frs_session = frs.filter(dataset__in=dsets)
            session_path = af.get_session_path(Path(_filename_from_file_record(frs_session.first())))
            eids.append(ses)
            try:
                gc.operation_ls(server_repository.globus_endpoint_id, path=session_path)
            except globus_sdk.TransferAPIError as e:
                if not "404, 'ClientError.NotFound'" in str(e):
                    raise e
                else:  # the directory doesn't exist on the target
                    continue
            dc.add_item(session_path)
        else:
            _logger.warning(f"session {ses} doesn't seem to have everything in sdsc")

    task = gc.submit_delete(dc)
    status = gc.task_wait(task['task_id'])
    frs2delete = frs.exclude(data_repository__globus_is_personal=False).filter(dataset__session__in=eids)
    _logger.info(f"removing {i} sessions representing {frs2delete.count()} file records on {labname}")
    frs2delete.delete()

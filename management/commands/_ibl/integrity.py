import datetime
import logging

from data.models import Dataset

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

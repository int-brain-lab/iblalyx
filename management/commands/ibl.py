from django.core.management import BaseCommand

from ._ibl.transfers import ftp_delete_local, ftp_upload
from ._ibl.monitor import monitor_dlc, monitor_spikesorting
from ._ibl.spreadsheets import histology_assign_update
from ._ibl.table import qc_table
from ._ibl.tasks import held_status_reset, task_reset, started_stalled_reset


class Command(BaseCommand):
    """
    Run in the following way

    python ./manage.py ibl ftp_delete_local
        Removes files on the FTP server that exist on the SDSC

    python ./manage.py ibl histology_assign_update
        Creates histology update spreadsheet to assign experimenters tasks

    python ./manage.py ibl monitor_dlc
        Appends a JSON string to a monitor DLC log to track progress on DLC compute

    python ./manage.py ibl monitor_spikesorting
        Makes a pqt table with spike sorting status

    python ./manage.py ibl qc_table
        Creates a QC pandas dataframe recap from sessions JSON

    python ./manage.py ibl task_reset --id {task_uuid}
        Resets a single task to waiting and clear log

    python ./manage.py ibl task_held_status_reset
        Resets to waiting held status tasks for which all parents passed

    python ./manage.py ibl task_started_stalled_reset
        Resets to waiting tasks that have been Started for more than 6 days
    """
    def add_arguments(self, parser):
        parser.add_argument('action', help='Action')
        parser.add_argument('--id', action='store', type=str, required=False)

    def handle(self, *args, **options):
        action = options.get('action')

        if action == 'ftp_delete_local':
            ftp_delete_local()
        elif action == 'histology_assign_update':
            histology_assign_update()
        elif action == 'monitor_dlc':
            monitor_dlc()
        elif action == 'qc_table':
            qc_table()
        elif action == 'task_held_status_reset':
            held_status_reset()
        elif action == 'task_started_stalled_reset':
            started_stalled_reset()
        elif action == 'task_reset':
            task_reset(options.get('tid'))
        elif action == 'monitor_spikesorting':
            monitor_spikesorting()
        else:
            raise ValueError(f'No action for command {action}')

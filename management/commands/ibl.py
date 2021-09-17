from django.core.management import BaseCommand

from ._ibl.integrity import ftp_delete_local
from ._ibl.monitor import monitor_dlc
from ._ibl.spreadsheets import histology_assign_update
from ._ibl.table import qc_table
from ._ibl.tasks import held_status_reset, task_reset


class Command(BaseCommand):
    """
    Run in the following way

    python ./manage.py ibl ftp_delete_local
        Removes files on the FTP server that exist on the SDSC

    python ./manage.py ibl held_status_reset
        Resets to waiting held status tasks for which all parents passed

    python ./manage.py ibl histology_assign_update
        Creates histology update spreadsheet to assign experimenters tasks

    python ./manage.py ibl monitor_dlc
        Appends a JSON string to a monitor DLC log to track progress on DLC compute

    python ./manage.py ibl qc_table
        Creates a QC pandas dataframe recap from sessions JSON

    python ./manage.py ibl task_reset --id {task_uuid}
        Resets a single task to waiting and clear log
    """
    def add_arguments(self, parser):
        parser.add_argument('action', help='Action')
        parser.add_argument('--id', action='store', type=str, required=False)

    def handle(self, *args, **options):
        action = options.get('action')

        if action == 'ftp_delete_local':
            ftp_delete_local()
        elif action == 'held_status_reset':
            held_status_reset()
        elif action == 'histology_assign_update':
            histology_assign_update()
        elif action == 'monitor_dlc':
            monitor_dlc()
        elif action == 'qc_table':
            qc_table()
        elif action == 'task_reset':
            task_reset(options.get('tid'))
        else:
            raise ValueError(f'No action for command {action}')

from django.core.management import BaseCommand

from ._ibl.integrity import ftp_delete_local
from ._ibl.monitor import monitor_dlc
from ._ibl.spreadsheets import histology_assign_update
from ._ibl.table import qc_table


class Command(BaseCommand):
    """
    Run in the following way
    python ./manage.py ibl ftp_delete_local
        Removes files on the FTP server that exist on the SDSC
    python ./manage.py ibl histology_assign_update

    python ./manage.py ibl monitor_dlc
    python ./manage.py ibl qc_table
    """
    def add_arguments(self, parser):
        parser.add_argument('action', help='Action')

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

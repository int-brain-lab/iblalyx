from django.core.management import BaseCommand

from ._ibl.transfers import ftp_delete_local
from ._ibl.monitor import monitor_dlc, monitor_spikesorting
from ._ibl.spreadsheets import histology_assign_update
from ._ibl.table import qc_table
from ._ibl.tasks import held_status_reset, task_reset, started_stalled_reset
from ._ibl.housekeeping import remove_sessions_local_servers
from ._ibl.paired_recordings import compute_upload_paired_experiments_to_s3


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

    python ./manage.py ibl task_started_stalled_reset
        Resets to waiting tasks that have been Started for more than 6 days

    python ./manage.py ibl cleanup_old_sessions --lab angelakilab --date 2020-06-01  --n 500
        Resets to waiting tasks that have been Started for more than 6 days

    python ./manage.py ibl paired_recordings
        Computes the latest version of the paired recording cache and uploads it to s3://[public_bucket]/caches/alyx
    """
    def add_arguments(self, parser):
        parser.add_argument('action', help='Action')
        parser.add_argument('--id', action='store', type=str, required=False)
        parser.add_argument('--date', action='store', type=str, required=False)
        parser.add_argument('--lab', action='store', type=str, required=False)
        parser.add_argument('--data-repository', type=str, help='data repository', required=False)
        parser.add_argument('--n', action='store', type=int, required=False, default=None)
        parser.add_argument('--dry', action='store_true', required=False, default=False)

    def handle(self, *args, **options):
        """
        :param args:
        :param options:
        :return:
        """
        action = options.get('action')
        defined_ops = {k for k, v in options.items() if v and k not in ['action', 'verbosity']}
        if action not in ('task_reset', 'cleanup_old_sessions') and defined_ops:
            raise ValueError(
                f'The following options are not supported for "{action}": ' + ", ".join(defined_ops)
            )
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
            if defined_ops != {'id'}:
                raise ValueError(
                    f'The following options are not supported for "{action}": ' +
                    ", ".join(o for o in defined_ops if o != 'id')
                )
            task_reset(options.get('id'))
        elif action == 'monitor_spikesorting':
            monitor_spikesorting()
        elif action == 'cleanup_old_sessions':
            if 'id' in defined_ops:
                raise ValueError(f'`id` not supported for action "{action}"')
            lab = options.get('lab')
            repo = options.get('data_repository')
            date = options.get('date')
            n = options.get('n') or 250
            dry = options.get('dry')
            remove_sessions_local_servers(
                lab, data_repository=repo, archive_date=date, nsessions=n, dry_run=dry)
        elif action == 'paired_recordings':
            compute_upload_paired_experiments_to_s3()
        else:
            raise ValueError(f'No action for command {action}')

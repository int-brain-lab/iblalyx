# Adapted from https://github.com/int-brain-lab/ibldevtools/blob/master/olivier/archive/2022/2022-03-14_trials_tables.py

"""
Generate per subject trials aggregate files for all culled subjects that have at least one session with an ibl project
and ibl task protocol.
Script meant to be run on SDSC, under the ibllib environment
TODOS:
-   list all datasets and do an aggregate of checksums + ids
"""

import logging
from pathlib import Path
import pandas as pd

from actions.models import Session
from subjects.models import Subject

from one.alf import io as alfio

# Paths
input_path = Path('/mnt/ibl')
output_path = Path('/mnt/ibl/aggregates/trials')
output_path.mkdir(exist_ok=True, parents=True)
# Whether overwrite existing output files
overwrite = True

# Prepare logger
logger = logging.getLogger('ibllib')
logger.setLevel(logging.INFO)
handler = logging.handlers.RotatingFileHandler(output_path.joinpath('trials_export.log'),
                                               maxBytes=(1024 * 1024 * 256), )
logger.addHandler(handler)

# Find all culled subjects with at least one session in an ibl project
subjects = Session.objects.filter(project__name__icontains='ibl',
                                  task_protocol__icontains='ibl').values_list('subject').distinct()
subjects = Subject.objects.filter(id__in=subjects, cull__isnull=False)

no_trials_table = []
no_path = []
other_errors = []
for i, sub in enumerate(subjects):
    try:
        print(f'{i}/{subjects.count()} {sub.nickname}')
        logger.info(f'SUBJECT: {sub.nickname}')
        out_file = output_path.joinpath(f"{sub.lab.name}_{sub.nickname}_trials.pqt")
        # ToDo: check on the checksum
        if out_file.exists():
            if overwrite is False:
                logger.warning(f"...{out_file} already exists and overwrite=False, skipping")
                continue
            elif overwrite is True:
                logger.warning(f"...{out_file} already exists and overwrite=True, overwriting existing file!")
        # Find all sessions of this subject
        sessions = Session.objects.filter(subject=sub, task_protocol__icontains='ibl')
        all_trials = []
        for ses in sessions:
            # Check if the session has any trials data
            if ses.data_dataset_session_related.filter(name__icontains='_ibl_trials.table').count() == 0:
                logger.error(f'...No trials data found for session {ses.id} - skipping')
                no_trials_table.append(str(ses.id))
                continue
            # Try and find expected session path on disk
            alf_path = input_path.joinpath(
                sub.lab.name,
                'Subjects',
                ses.subject.nickname,
                ses.start_time.strftime('%Y-%m-%d'),
                f'{ses.number:03d}',
                'alf'
            )
            if not alf_path.exists():
                logger.error(f"...Alf path does not exist for session {ses.id} - skipping")
                no_path.append(str(ses.id))
                continue
            # load trials table
            trials = alfio.load_object(alf_path, 'trials', attribute='table', short_keys=True)
            trials = trials.to_df()

            # Add to list of trials for subject
            trials['session'] = str(ses.id)
            trials['session_start_time'] = ses.start_time
            trials['session_number'] = ses.number
            trials['task_protocol'] = ses.task_protocol
            all_trials.append(trials)

        # Concatenate trials from all sessions for subject
        if len(all_trials) == 0:
            logger.error(f"...No sessions with trials data found for subject {sub.nickname} - skipping")
            continue
        df_trials = pd.concat(all_trials, ignore_index=True)
        df_trials.to_parquet(out_file)
        logger.info(f"...Exported {out_file}")
    except Exception as e:
        logger.error(f"...Error for subject {sub.nickname}: {e}")
        other_errors.append(str(sub.nickname))
        continue

if len(no_trials_table) > 0:
    pd.DataFrame(columns=['eid'], data=no_trials_table).to_csv(output_path.joinpath('no_trials_table.csv'))
    logger.info(f'Wrote list of sessions with no trials table to {output_path.joinpath("no_trials_table.csv")}')

if len(no_path) > 0:
    pd.DataFrame(columns=['eid'], data=no_path).to_csv(output_path.joinpath('no_path.csv'))
    logger.info(f'Wrote list of sessions with no alf path to {output_path.joinpath("no_path.csv")}')

if len(other_errors) > 0:
    pd.DataFrame(columns=['eid'], data=other_errors).to_csv(output_path.joinpath('other_errors.csv'))
    logger.info(f'Wrote list of subjects with other errors to {output_path.joinpath("other_errors.csv")}')
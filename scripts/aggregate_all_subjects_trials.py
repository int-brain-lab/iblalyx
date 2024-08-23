"""Script for generating subject trials table for recently culled subjects.

Must be run within the alyx environment.
"""
import os
import argparse
import django
import logging
import shutil
from pathlib import Path

import pandas as pd
from iblutil.util import log_to_file

from django.db.models import Count, Q
from actions.models import Session
from subjects.models import Subject
from data.models import Dataset
from data.management.commands.aggregate_subject_trials import Command, OUTPUT_PATH, ROOT, logger


class LogToFile:
    """Context manager for modifying logging file handler."""
    def __init__(self, log, filename):
        self.log = logging.getLogger(log) if isinstance(log, str) else log
        filename.parent.mkdir(exist_ok=True, parents=True)
        self.filename = filename

    def _cleanup_handler(self):
        if fh := next(filter(lambda h: isinstance(h, logging.FileHandler), self.log.handlers), None):
            fh.close()
            self.log.removeHandler(fh)

    def __enter__(self):
        self._cleanup_handler()
        log_to_file(self.log, filename=self.filename)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._cleanup_handler()


if __name__ == '__main__':
    # Initialize Django apps
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'alyx.settings')
    django.setup()

    parser = argparse.ArgumentParser(
        prog='SubjectsTrialsAggregates', description='Generate subject trials aggregate tables for all subjects.',
        epilog='See also: iblalyx/management/commands/aggregate_subject_trials.py')
    parser.add_argument('--only-new-subjects', action='store_true',
                        help='Whether to only create trial aggregates for subjects that don\'t have one')
    args = parser.parse_args()

    # Location of log file handler output (is later moved to OUTPUT_PATH) and JSON containing map of processed subjects
    logdir = Path.home().joinpath('ibl_logs', 'subject_trials_aggragates')

    # Find all culled subjects with at least one session in an ibl project
    sessions = Session.objects.filter(projects__name__icontains='ibl')
    subjects = Subject.objects.filter(id__in=sessions.values_list('subject'), cull__isnull=False).exclude(
        nickname__icontains='test')
    # Also make sure to only keep subjects that have at least one session with ibl task protocol and a trials table
    sessions = Session.objects.filter(subject__in=subjects, task_protocol__icontains='ibl')
    sessions = sessions.annotate(trials_table_count=Count('data_dataset_session_related',
                                                          filter=Q(data_dataset_session_related__name='_ibl_trials.table.pqt')))
    sessions = sessions.exclude(trials_table_count=0)
    subjects = Subject.objects.filter(id__in=sessions.values_list('subject'))

    # If only new, exclude all subjects that already have an aggregate
    # existing files with this file name
    if args.only_new_subjects:
        table_exists = Dataset.objects.filter(name='_ibl_subjectTrials.table.pqt').values_list('object_id', flat=True)
        subjects = subjects.exclude(id__in=table_exists)

    # Go through subjects and check if aggregate needs to be (re)created
    logger.info(f'Processing {subjects.count()} subjects')
    # Arguments to pass to management command handler
    kwargs = dict(
        verbosity=1, dryrun=False, alyx_user='miles', output_path=OUTPUT_PATH, clobber=False, data_path=ROOT,
        default_revision=None, revision=None, training_status=False)

    for i, sub in enumerate(subjects):
        nickname = sub.nickname
        logger.info('=============== Processing %s (%i/%i) ===============', nickname, i, subjects.count())
        with LogToFile(logger, logdir / nickname):
            try:
                dset, out_file, log_file = Command().handle(subject=nickname, **kwargs)
            except Exception as ex:
                logger.error(ex)
        # Move log file to the location of the output dataset
        shutil.move(logdir.joinpath(nickname), log_file.parent / log_file.stem)  # _ibl_subjectTrials.log
        outcomes = pd.read_csv(log_file, index_col='Unnamed: 0')
        logger.info('%i/%i sessions successfully processed for %s',
                    sum(~outcomes.notes.isin(('no trials tasks for this session', 'SUCCESS'))), len(outcomes), nickname)

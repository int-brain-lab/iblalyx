import logging

from django.db.models import Count, F, Q

from jobs.models import Task

logger = logging.getLogger('data.transfers')
logger.setLevel(20)


def held_status_reset():
    # looks for held statuses task if parents are all complete or waiting, in which case reset to waiting
    t = Task.objects.filter(status=25)
    t = t.annotate(n_parents=Count('parents'), n_parents_ok=Count('parents', filter=Q(status=60) | Q(status=20)))
    t = t.annotate(n_parents_ko=F('n_parents') - F('n_parents_ok'))
    t_reset = t.filter(n_parents_ko=0)
    logger.info(f'reset {t_reset.count()} held tasks to waiting with parents')
    _reset_queryset(t_reset)


def task_reset(task_ids):
    t = Task.objects.filter(id='38bee792-1f8e-485e-86f8-6732bf999098')
    _reset_queryset(t)


def _reset_queryset(tqs):
    logger.info(f'reset {tqs.count()} tasks')
    tqs.update(log=None, status=20, version=None, time_elapsed_secs=None)

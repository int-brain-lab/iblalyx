import datetime
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


def started_stalled_reset():
    # after 6 days we consider the job stalled
    cut_off = datetime.datetime.now() - datetime.timedelta(days=6)
    t = Task.objects.filter(status=30, datetime__lt=cut_off)
    _reset_queryset(t)


def task_reset(task_id):
    t = Task.objects.filter(id=task_id)
    _reset_queryset(t)


def _reset_queryset(tqs):
    logger.info(f'reset {tqs.count()} tasks')
    tqs.update(log=None, status=20, version=None, time_elapsed_secs=None)

# cortexlab version old Django
# while held_remaining:
#     held_remaining = False
#     for t in Task.objects.filter(status=25):
#         pok = t.parents.values_list('status', flat=True)
#         if set(pok).issubset(set([20, 60])):
#             t.status = 20
#             t.save()
#         elif set(pok).issubset(set([20, 60, 25])):
#             held_remaining = True


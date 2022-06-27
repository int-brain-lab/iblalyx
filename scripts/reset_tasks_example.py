import logging
from jobs.models import Task

logger = logging.getLogger('data.transfers')
logger.setLevel(20)

def _reset_queryset(tqs):
    logger.info(f'reset {tqs.count()} tasks')
    tqs.update(log=None, status=20, version=None, time_elapsed_secs=None)

t = Task.objects.filter(name__icontains='SpikeSorting', status=40, log__icontains="'Bunch' object has no attribute 'h'")

_reset_queryset(t)

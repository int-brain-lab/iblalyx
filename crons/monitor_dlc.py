from datetime import datetime
import json

from jobs.models import Task
from django.db.models import Avg, Count
# status - 20: Waiting / 25: Held / 30: Started / 40: Errored / 45: Abandoned / 50: Empty / 60: Complete
DCHOICES = {c[0]:c[1] for c in Task.status.field.choices}
count = Task.objects.filter(name='EphysDLC').values('status').annotate(n=Count('status'))

d = {DCHOICES[c['status']]:c['n'] for c in count}
t = datetime.now()
print(t, json.dumps(d))

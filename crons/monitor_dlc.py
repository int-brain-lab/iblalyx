from datetime import datetime
import json

from jobs.models import Task
from django.db.models import Avg, Count
# status - 20: Waiting / 25: Held / 30: Started / 40: Errored / 45: Abandoned / 50: Empty / 60: Complete


count = Task.objects.filter(name='EphysDLC').values('status').annotate(n=Count('status'))

d = {}
for c in count:
    k = next(ch[1] for ch in Task.status.field.choices if ch[0] == c['status'])
    d[k] = c['n']

t = datetime.now()
print(t, json.dumps(d))

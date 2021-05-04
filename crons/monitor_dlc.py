from datetime import datetime
from jobs.models import Task
c = Task.objects.filter(name='EphysDLC', status=50).count()
t = datetime.now()
print(t, c)

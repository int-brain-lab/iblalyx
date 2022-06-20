"""Find duplicate sessions.

List sessions where subject/date/number is not unique in the database
"""
# https://stackoverflow.com/questions/54249645/how-to-find-duplicate-records-based-on-certain-fields-in-django
import pandas as pd

from actions.models import Session
from django.db.models import Count, F, CharField, Value as V
from django.db.models.functions import Concat

model = Session.objects.select_related('subject')
qs = model.annotate(exp_ref=Concat(
    F('subject__nickname'), V('/'), F('start_time__date'), V('/'), F('number'), output_field=CharField()
))
dupes = qs.values('exp_ref').annotate(dupe_count=Count('exp_ref')).filter(dupe_count__gt=1)

duplicates = (pd.DataFrame(list(dupes.all().values('exp_ref', 'dupe_count')))
              .set_index('exp_ref')
              .sort_values('dupe_count', axis=0, ascending=False))

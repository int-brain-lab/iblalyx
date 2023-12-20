from actions.models import Session
from misc.models import Note
from data.models import Dataset, Tag
from django.db.models import Q
import pandas as pd


passive = Session.objects.filter(
    ~Q(json__sign_off_checklist___passiveChoiceWorld_01=None),
    json__sign_off_checklist___passiveChoiceWorld_01__isnull=False,
)
bad_passive = Note.objects.filter(text__icontains='=== SIGN-OFF NOTE FOR _passive').values_list('object_id', flat=True)
passive = passive.exclude(id__in=bad_passive)


dtypes = [
    '_iblrig_RFMapStim.raw',
    '_ibl_passivePeriods.intervalsTable',
    '_ibl_passiveRFM.times',
    '_ibl_passiveGabor.table',
    '_ibl_passiveStims.table'
    ]
dsets = Dataset.objects.filter(session__in=passive, dataset_type__name__in=dtypes, default_dataset=True)

tag, _ = Tag.objects.get_or_create(name="2023_Q4_IBL_et_al_BWM_passive", protected=True, public=True)
tag.datasets.set(dsets)

# Save dataset IDs
dset_ids = [str(d.id) for d in dsets]
df = pd.DataFrame(dset_ids, columns=['dataset_id'])
df.to_parquet('./2023_Q4_IBL_et_al_BWM_passive_datasets.pqt')
"""
Data Release request link:
https://docs.google.com/document/d/1ziHSzUoGWHMi8YU3JtCr2Q8QTlbZUllmtxxarEY9ab4/edit?tab=t.0
"""
# %%
import sys
from pathlib import Path

import pandas as pd
from django.db.models import Q

from data.models import Dataset, Tag
from actions.models import Session
import alyx.base

IBL_ALYX_ROOT = Path(alyx.base.__file__).parents[3].joinpath('iblalyx')
assert IBL_ALYX_ROOT.exists(), 'No IBL_ALYX_ROOT found, it is usually at the same directory level as the alyx repo'
sys.path.append(str(IBL_ALYX_ROOT.parent))

import iblalyx.releases.utils

eids = ['022dd14c-eff2-470f-863c-e019fafa53ae',
 '078fb4b2-4bff-414c-92a8-a2fb97ffcf59',
 '0fe99726-9982-4c41-a07c-2cd7af6a6733',
 '107249ca-0d03-4e56-a7eb-6fe6210550ae',
 '11cc0294-fbc5-44b7-8a2c-484daa64c81e',
 '150f92bc-e755-4f54-96c1-84e1eaf832b4',
 '27f3c7a6-7be5-40e2-b4d8-9393978aeae1',
 '2cff323c-1510-4b78-a5d1-ca07b203f60c',
 '2d768cde-65d4-4374-af2e-6ff3bf606eb4',
 '2eb86e84-4b48-488c-81ed-b98335d9a922',
 '308274fc-28e8-4bfd-a4e3-3903b7b48c28',
 '38bdc37b-c8be-4f18-b0a2-8a22dfa5f47e',
 '3a1b819b-71ef-4d71-aae6-9f83c1f509cb',
 '41dfdc2a-987a-402a-99ae-779d5f569566',
 '48cdc3ce-8e21-4090-9686-e26c6e4e851f',
 '531e7ac0-cfcd-4593-9bf7-bb7bab5d66e9',
 '5c936319-6829-41cb-abc7-c4430910a6a0',
 '6f321eab-6dad-4f2e-8160-5b182f999bb6',
 '6f87f78d-f091-46c7-8226-e8b1936b28ee',
 '78fceb60-e623-431b-ab80-7e29209058ac',
 '7aa9fe27-3f10-4ee0-a5a3-a0c59884f2b6',
 '7ae3865a-d8f4-4b73-938e-ddaec33f8bc6',
 '804bc680-976b-4e3e-9a47-a7e94847bd06',
 '83292b0f-e30f-48e1-ad0a-6f2bfe04e8b0',
 '87b628a4-f11a-429c-ad98-34d43cf3178b',
 '89e258e9-cbca-4eca-bac4-13a2388b5113',
 '8cfb0b3d-2877-4616-9e32-4139c4501691',
 '93374502-c701-4b83-aa1a-23050b514708',
 '945028b5-bb38-4379-8ae4-488bcd67bcf5',
 '9931191e-8056-4adc-a410-a4a93487423f',
 '9a14e9b7-0f79-410b-a456-1e8e7887e621',
 '9b4f6a8d-c879-4348-aa7e-0b34f6c6dacb',
 'a06189b0-a66e-4a5a-a1ef-4afa80de8b31',
 'a0dfbbc6-0454-4dc6-ade0-9ba57c18241d',
 'a3470924-a5b0-4cee-a04e-7597d4a94f8d',
 'a44fd8cc-ae4c-49b2-a6b4-97c6552ad9f6',
 'a45e62df-9f7f-4429-95a4-c4e334c8209f',
 'a5145869-a54a-4871-95ef-016421122844',
 'a68ef902-026c-4dfa-857f-8bc799a3b5e5',
 'ab8a5331-1d0f-4b8a-9e0f-7be41c4857f9',
 'ac03969c-3b66-42cc-b23e-edaa566aff46',
 'ad8e802d-ce83-437a-865f-fa769762a602',
 'af74b29d-a671-4c22-a5e8-1e3d27e362f3',
 'b26295df-e78d-4368-b694-1bf584f25bfc',
 'ba7fc4d0-0486-4415-9b12-3f13b1cff710',
 'bb2153e7-1052-491e-a022-790e755c7a54',
 'bf358c9a-ef84-4604-b83a-93416d2827ff',
 'c875fc7d-0966-448a-813d-663088fbfae8',
 'c90cdfa0-2945-4f68-8351-cb964c258725',
 'c94463ed-57da-4f02-8406-46f2f03924f3',
 'da9eeafc-d7af-4a19-bf1c-2064e5b1b696',
 'ded7c877-49cf-46ad-b726-741f1cf34cef',
 'e38c3ca1-4c0e-4fac-bcaf-b94db6e1b8e0',
 'e71bd25a-8c1e-4751-8985-4463a91c1b66',
 'f2545193-1c5c-420e-96ac-3cb4b9799ea5',
 'f31752a8-a6bb-498b-8118-6339d3d74ecb',
 'f45e30cf-12aa-4fa0-8248-f9f885dfa9ef',
 'fe0ecca9-9279-4ce6-bbfe-8b875d30d34b',
 'fe80df7d-15f0-4f89-9bbb-d3e5725c4b0a']

# %%
TAG_NAME = '2025_Q3_Zang_et_al_Aging'
DRY_RUN = True

sess = Session.objects.filter(id__in=eids)
df_datasets = []

# video datasets: we exlude QC critical datasets and include lick times
dsets = iblalyx.releases.utils.get_video_datasets_for_ephys_sessions(eids, cam_labels=['left', 'right', 'body'])
df_datasets.append(pd.DataFrame([str(eid) for eid in dsets.values_list('pk', flat=True)], columns=['dataset_id']))

# behaviour and wheel datasets
dsets = Dataset.objects.filter(session__in=sess, dataset_type__name__in=iblalyx.releases.utils.DTYPES_RELEASE_BEHAVIOUR)
df_datasets.append(pd.DataFrame([str(eid) for eid in dsets.values_list('pk', flat=True)], columns=['dataset_id']))

# ephys datasets
dsets = Dataset.objects.filter(session__in=sess, dataset_type__name__in=iblalyx.releases.utils.DTYPES_RELEASE_EPHYS_ALL)
df_datasets.append(pd.DataFrame([str(eid) for eid in dsets.values_list('pk', flat=True)], columns=['dataset_id']))

# finalize
df_datasets = pd.concat(df_datasets, axis=1)

# %% Tagging in production database
if DRY_RUN is False:
    dsets2tag = Dataset.objects.filter(id__in=df_datasets['dataset_id'])
    tag, _ = Tag.objects.get_or_create(name=TAG_NAME, protected=True, public=True)
    tag.datasets.set(dsets2tag)

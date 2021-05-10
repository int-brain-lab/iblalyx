from misc.models import LabMember

for lm in LabMember.objects.all():
    if lm.username == 'root':
        continue
    lm.email = ""
    lm.username = str(lm.id)[:8]
    lm.first_name = ''
    lm.last_name = ''
    lm.save()

ibl = LabMember.objects.create(username='intbrainlab')


from data.models import DataRepository
for dr in DataRepository.objects.all():
    dr.data_url = dr.data_url.replace('.org/', '.org/public/')
    dr.globus_path = '/public' + dr.globus_path
    dr.save()

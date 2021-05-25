# source /var/www/alyx-dev/venv/bin/activate
# /var/www/alyx-dev/alyx/manage.py shell < SessionCount.py

from actions.models import Session
from misc.models import Lab
from data.models import DataRepository, FileRecord
from subjects.models import Subject

nfiles = 0
for dr in DataRepository.objects.all():
    fr = FileRecord.objects.filter(data_repository=dr).filter(exists=True)
    print(str(fr.count()).rjust(5) + '  files   ' + dr.name)
    if 'flatiron' in dr.name:
        nfiles += fr.count()
print('\n'*3)

nses = 0
nfiles = 0
for lab in Lab.objects.all():
    ses = Session.objects.filter(lab=lab)
    nses += ses.count()
    nfil = 0
    for dr in lab.repositories.filter(name__icontains='flatiron'):
        nfil += FileRecord.objects.filter(data_repository=dr).filter(exists=True).count()
    nfiles += nfil
    print(lab.name,
          '\n\t', str(ses.count()).rjust(5) + '  sessions',
          '\n\t', str(nfil).rjust(5) + '  files on FlatIron',
          '\n')

nephys = Session.objects.filter(procedures__name__icontains='ephys').count()

print(str(nses).rjust(6), ' Total sessions')
print(str(nephys).rjust(6), ' Ephys sessions')
print(str(nfiles).rjust(6), ' Total files on FlatIron')
print(Subject.objects.all().count(), ' Total Subjects')

# nsubs = Session.objects.all().values_list('subject', flat=True).distinct().count()
nsubs = Session.objects.filter(subject__species__name='Mus musculus').values_list('subject').distinct().count()
print(nsubs, 'Mouse subjects with sessions')

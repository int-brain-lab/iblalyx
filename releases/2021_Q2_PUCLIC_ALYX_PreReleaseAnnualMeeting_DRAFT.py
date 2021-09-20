from django.db.models import Q

from alyx import settings
from data.models import Dataset, FileRecord, DataRepository
from actions.models import Session
from subjects.models import Subject, Project
from experiments.models import ProbeInsertion, TrajectoryEstimate
from misc.models import Lab, LabMember

# DO NOT RUN THIS ON THE PRODUCTION DATABASE - ONLY OPENALYX
assert 'alyx.internationalbrainlab.org' not in settings.ALLOWED_HOSTS, "Oups, looks like you're trying to run on the production database"
assert 'openalyx.internationalbrainlab.org' in settings.ALLOWED_HOSTS, "This runs exclusively on the public database"

pids = ["da8dfec1-d265-44e8-84ce-6ae9c109b8bd",  # SWC_043_2020-09-21_probe00 ok
        "b749446c-18e3-4987-820a-50649ab0f826",  # KS023_2019-12-10_probe01  ok
        "f86e9571-63ff-4116-9c40-aa44d57d2da9",  # CSHL049_2020-01-08_probe00 a bit stripy but fine
        "675952a4-e8b3-4e82-a179-cc970d5a8b01"]  # CSH_ZAD_029_2020-09-19_probe01 a bit stripy as well]

probe_insertions = ProbeInsertion.objects.filter(id__in=pids)
# remove the datasets from other probes
for ins in probe_insertions:
    other_probes = ProbeInsertion.objects.filter(session=ins.session).exclude(pk=ins.pk)
    Dataset.objects.filter(probe_insertion__in=other_probes).delete()

print('prune probe insertions')
ProbeInsertion.objects.exclude(id__in=pids).delete()

print('prune sessions')
Session.objects.exclude(id__in=probe_insertions.values_list('session', flat=True)).delete()
sessions = Session.objects.all()

print('prune datasets')
Dataset.objects.exclude(session__in=sessions).delete()  # this is redundant with the above
datasets = Dataset.objects.all()

print('prune filerecords')
# get only filerecords and repositories that are on the flatiron
data_repositories = DataRepository.objects.filter(globus_is_personal=False)
DataRepository.objects.exclude(globus_is_personal=False).delete()
file_records = FileRecord.objects.filter(dataset__in=datasets, data_repository__in=data_repositories)
file_records = FileRecord.objects.all()

print('prune subjects')
Subject.objects.exclude(actions_sessions__in=sessions).delete()
subjects = Subject.objects.all()

print('prune projects]')
projects = Project.objects.filter(
    Q(pk__in=sessions.values_list('project', flat=True).distinct()) |
    Q(pk__in=subjects.values_list('projects', flat=True).distinct())
)
Project.objects.exclude(pk__in=projects.values('pk')).delete()


# species = Species.objects.filter(subject__in=subjects).distinct()
# strains = Strain.objects.filter(subject__in=subjects).distinct()
# alleles = Allele.objects.filter(subject__in=subjects).distinct()
# sequences = Sequence.objects.filter(subject__in=subjects).distinct()
# lines = Line.objects.filter(subject__in=subjects).distinct()
# litters = Litter.objects.filter(subject__in=subjects).distinct()
# breeding_pairs = BreedingPair.objects.filter(line_id__in=lines).distinct()
# housings = Housing.objects.filter(subjects__in=subjects)
# zygosities = Zygosity.objects.filter(allele_id__in=alleles)

# get only ephys aligned histology track
trajectory_estimates = TrajectoryEstimate.objects.filter(probe_insertion__in=probe_insertions, provenance=70)
TrajectoryEstimate.objects.exclude(pk__in=trajectory_estimates.values('id')).delete()

labs = Lab.objects.filter(pk__in=sessions.values_list('lab', flat=True))
Lab.objects.exclude(pk__in=labs).delete()

lab_members = LabMember.objects.filter(
    Q(pk__in=sessions.values_list('users', flat=True).distinct()) |
    Q(pk__in=datasets.values_list('created_by_id', flat=True).distinct())
)
LabMember.objects.exclude(pk__in=lab_members).delete()


# Delete some models
from actions.models import Weighing, WaterAdministration
from jobs.models import Task
from reversion.models import Version

Weighing.objects.all().delete()
WaterAdministration.objects.all().delete()
Task.objects.all().delete()
Version.objects.all().delete()

# user: intbrainlab
# PW: international
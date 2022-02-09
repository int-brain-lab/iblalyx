from datetime import datetime
import pandas as pd

from django.db.models import Q
from django.contrib.auth.models import Group

from misc.models import LabMember, Lab, LabMembership, LabLocation, Note, CageType, \
    Enrichment, Food, Housing, HousingSubject
from actions.models import Session, ProcedureType, Weighing, WaterType, WaterAdministration, VirusInjection, \
    ChronicRecording, Surgery, WaterRestriction, Notification, NotificationRule, CullReason, CullMethod, Cull
from data.models import DataRepositoryType, DataRepository, DataFormat, DatasetType, Tag, Revision, Dataset, \
    FileRecord, Download
from subjects.models import Project, Subject, SubjectRequest, Litter, BreedingPair, Line, Species, Strain, Source, \
    ZygosityRule, Allele, Zygosity, Sequence, GenotypeTest
from experiments.models import BrainRegion, CoordinateSystem, ProbeModel, ProbeInsertion, TrajectoryEstimate, Channel
from jobs.models import Task

public_ds_files = ['2021_Q2_ErdemPaper.csv', '2021_Q2_MattPaper.csv', '2021_Q2_PreReleaseAnnualMeeting.csv']
public_ds_tags = ["Behaviour Paper", "Erdem's paper", "Matt's paper", "May 2021 pre-release"]

public_ds_ids = []
for f in public_ds_files:
    public_ds_ids.extend(list(pd.read_csv(f, index_col=0)['dataset_id']))

# Delete all datasets that are not in that list, along with their file records
Dataset.objects.using('public').exclude(pk__in=public_ds_ids).delete()
datasets = Dataset.objects.using('public').all()

# Delete tags that aren't in the list above (released datasets might have additional, not-yet-released tags)
Tag.objects.using('public').exclude(name__in=public_ds_tags).delete()

# Delete personal data repositories and associated file records
DataRepository.objects.using('public').exclude(globus_is_personal=False).delete()
# Replace some information in the data repositories
for dr in DataRepository.objects.using('public').all():
    if 'flatiron' in dr.hostname:
        dr.data_url = dr.data_url.replace('.org/', '.org/public/')
        dr.globus_path = '/public' + dr.globus_path
        dr.save()
    elif 'aws' in dr.hostname:
        dr.json = {'bucket_name': 'ibl-brain-wide-map-public'}
        dr.save()

# Remove unused dataset types formats and revisions
dataset_types = datasets.values_list('dataset_type', flat=True).distinct()
DatasetType.objects.using('public').exclude(pk__in=dataset_types).delete()

data_formats = datasets.values_list('data_format', flat=True).distinct()
DataFormat.objects.using('public').exclude(pk__in=data_formats).delete()

revisions = datasets.values_list('revision', flat=True).distinct()
Revision.objects.using('public').exclude(pk__in=revisions).delete()

# Delete sessions that don't have a dataset in public db, along with probe insertions, trajectories, channels and tasks
Session.objects.using('public').exclude(id__in=datasets.values_list('session_id', flat=True)).delete()
sessions = Session.objects.using('public').all()
# Remove identifying information from session json
for sess in sessions:
    sess.json['PYBPOD_CREATOR'] = []
    sess.json['PYBPOD_SUBJECT_EXTRA'] = {}
    sess.narrative = ''
    sess.save()

# Delete all subjects that don't have a session along with many actions
Subject.objects.using('public').exclude(actions_sessions__in=sessions).delete()
subjects = Subject.objects.using('public').all()

# Delete projects that aren't attached to public session or subject
projects = Project.objects.using('public').filter(
    Q(pk__in=sessions.values_list('project', flat=True).distinct()) |
    Q(pk__in=subjects.values_list('projects', flat=True).distinct())
)
Project.objects.using('public').exclude(pk__in=projects.values('pk')).delete()

# Delete Labs and LabMembers that aren't used
labs = Lab.objects.using('public').filter(pk__in=sessions.values_list('lab', flat=True))
Lab.objects.using('public').exclude(pk__in=labs).delete()

lab_members = LabMember.objects.using('public').filter(
    Q(pk__in=sessions.values_list('users', flat=True).distinct()) |
    Q(pk__in=datasets.values_list('created_by_id', flat=True).distinct())
)
LabMember.objects.using('public').exclude(pk__in=lab_members).delete()

# Anonymize remaining lab members
for lm in lab_members:
    if lm.username == 'root':
        continue
    lm.is_staff = False
    lm.is_superuser = False
    lm.email = ""
    lm.username = str(lm.id)[:8]
    lm.first_name = ''
    lm.last_name = ''
    lm.date_joined = datetime.now()
    lm.last_login = datetime.now()
    lm.password = ''
    try:
        lm.auth_token.delete()
    except (AssertionError, LabMember.auth_token.RelatedObjectDoesNotExist):
        pass
    lm.save()
# Create public user
public_user = LabMember.objects.using('public').create(username='intbrainlab',
                                                       is_active=True,
                                                       is_staff=True)
public_user.set_password('international')
public_user.groups.add(Group.objects.using('public').filter(name='Lab members')[0])
public_user.save()

# Delete all remaining probe insertions that don't have datasets in the public database
probeinsertions = ProbeInsertion.objects.using('public').filter(
    datasets__pk__in=datasets.values_list('pk', flat=True)).distinct()
ProbeInsertion.objects.using('public').exclude(pk__in=probeinsertions.values_list('pk', flat=True)).delete()

# Keep only ephys aligned histology track that have probe insertion in public db now
trajectories = TrajectoryEstimate.objects.using('public').filter(
    probe_insertion__in=probeinsertions).filter(provenance=70).distinct()
TrajectoryEstimate.objects.using('public').exclude(pk__in=trajectories.values_list('pk', flat=True)).delete()

# Remove unused Probe models, coordinate systems, channels and Brain Regions
ProbeModel.objects.using('public').exclude(pk__in=probeinsertions.values_list('model', flat=True).distinct()).delete()
CoordinateSystem.objects.using('public').exclude(
    pk__in=trajectories.values_list('coordinate_system', flat=True)).delete()
Channel.objects.using('public').exclude(trajectory_estimate__in=trajectories).delete()
BrainRegion.objects.using('public').exclude(
    pk__in=Channel.objects.using('public').values_list('brain_region', flat=True)).delete()

# Delete some misc models
LabMembership.objects.using('public').all().delete()
LabLocation.objects.using('public').all().delete()
Housing.objects.using('public').all().delete()
HousingSubject.objects.using('public').all().delete()
Note.objects.using('public').all().delete()
CageType.objects.using('public').all().delete()
Enrichment.objects.using('public').all().delete()
Food.objects.using('public').all().delete()

# Delete most action models
ProcedureType.objects.using('public').all().delete()
Weighing.objects.using('public').all().delete()
WaterType.objects.using('public').all().delete()
WaterAdministration.objects.using('public').all().delete()
VirusInjection.objects.using('public').all().delete()
ChronicRecording.objects.using('public').all().delete()
Surgery.objects.using('public').all().delete()
WaterRestriction.objects.using('public').all().delete()
Notification.objects.using('public').all().delete()
NotificationRule.objects.using('public').all().delete()
CullReason.objects.using('public').all().delete()
CullMethod.objects.using('public').all().delete()
Cull.objects.using('public').all().delete()

# Delete data download model
Download.objects.using('public').all().delete()

# Delete some subjects model
SubjectRequest.objects.using('public').all().delete()
Litter.objects.using('public').all().delete()
BreedingPair.objects.using('public').all().delete()
Line.objects.using('public').all().delete()
Species.objects.using('public').all().delete()
Strain.objects.using('public').all().delete()
Source.objects.using('public').all().delete()
ZygosityRule.objects.using('public').all().delete()
Allele.objects.using('public').all().delete()
Zygosity.objects.using('public').all().delete()
Sequence.objects.using('public').all().delete()
GenotypeTest.objects.using('public').all().delete()

# Delete Tasks
Task.objects.using('public').all().delete()
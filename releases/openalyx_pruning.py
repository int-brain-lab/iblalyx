import json
import pandas as pd
from datetime import datetime
from pathlib import Path

from django.db.models import Q
from django.contrib.auth.models import Group
from data.transfers import _add_uuid_to_filename

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


"""
Settings and Inputs
"""
# Adapt this for new releases
dtypes_exclude = ['_iblrig_taskSettings.raw']
public_ds_files = ['2021_Q1_BehaviourPaper_datasets.csv',
                   '2021_Q2_ErdemPaper_datasets.csv',
                   '2021_Q2_MattPaper_datasets.csv',
                   '2021_Q2_PreReleaseAnnualMeeting_datasets.csv']
public_ds_tags = ["Behaviour Paper",
                  "Erdem's paper",
                  "Matt's paper",
                  "May 2021 pre-release"]

# Get public aws information from local file to avoid storing this on github
aws_info_file = '/home/datauser/Documents/aws_public_info.json'
with open(aws_info_file) as f:
    aws_info = json.load(f)

print(f"Dataset IDs from files: {public_ds_files}")
print(f"Dataset types to exclude: {dtypes_exclude}")
print(f"Tags to keep: {public_ds_tags}\n")

# Load all datasets ids into one list
public_ds_ids = []
for f in public_ds_files:
    public_ds_ids.extend(list(pd.read_csv(f, index_col=0)['dataset_id']))

"""
Pruning and anonymizing database
"""
print(f"\nStarting to prune public database")
print("...pruning datasets")
# Delete all datasets that are not in that list, along with their file records, also datasets with to be excluded types
Dataset.objects.using('public').exclude(pk__in=public_ds_ids).delete()
Dataset.objects.using('public').filter(dataset_type__name__in=dtypes_exclude).delete()
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
        dr.json = {}
        dr.save()
    elif 'aws' in dr.hostname:
        dr.json = aws_info
        dr.save()

# Remove unused dataset types, formats and revisions
DatasetType.objects.using('public').exclude(pk__in=datasets.values_list('dataset_type', flat=True)).delete()
DataFormat.objects.using('public').exclude(pk__in=datasets.values_list('data_format', flat=True)).delete()
Revision.objects.using('public').exclude(pk__in=datasets.values_list('revision', flat=True)).delete()

print("...pruning sessions")
# Delete sessions that don't have a dataset in public db, along with probe insertions, trajectories, channels and tasks
Session.objects.using('public').exclude(id__in=datasets.values_list('session_id', flat=True)).delete()
sessions = Session.objects.using('public').all()
# Remove identifying information from session json
for sess in sessions:
    sess.json['PYBPOD_CREATOR'] = []
    sess.json['PYBPOD_SUBJECT_EXTRA'] = {}
    sess.narrative = ''
    sess.save()

print("...pruning subjects")
# Delete all subjects that don't have a session along with many actions
Subject.objects.using('public').exclude(actions_sessions__in=sessions).delete()
subjects = Subject.objects.using('public').all()

print("...pruning projects")
# Delete projects that aren't attached to public session or subject
projects = Project.objects.using('public').filter(
    Q(pk__in=sessions.values_list('project', flat=True).distinct()) |
    Q(pk__in=subjects.values_list('projects', flat=True).distinct())
)
Project.objects.using('public').exclude(pk__in=projects.values('pk')).delete()

print("...pruning labs and labmembers, anonymizing lab members")
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

print("...pruning probe insertions")
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

"""
Deleting some tables altogether
"""
print("...deleting some tables")
# misc
LabMembership.objects.using('public').all().delete()
LabLocation.objects.using('public').all().delete()
Housing.objects.using('public').all().delete()
HousingSubject.objects.using('public').all().delete()
Note.objects.using('public').all().delete()
CageType.objects.using('public').all().delete()
Enrichment.objects.using('public').all().delete()
Food.objects.using('public').all().delete()

# actions
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

# data
Download.objects.using('public').all().delete()

# subjects
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

# jobs
Task.objects.using('public').all().delete()
print("Finished pruning database\n")

'''
Create symlinks in public flatiron
'''
print("Starting to create symlinks")
for dset in datasets:
    fr = dset.file_records.filter(data_repository__name__startswith='flatiron').first()
    if fr is None:
        print(f"...no file record for dataset with ID: {str(dset.pk)}")
    else:
        rel_path = Path(fr.data_repository.globus_path).joinpath(fr.relative_path).relative_to('/public')
        rel_path = _add_uuid_to_filename(str(rel_path), dset.pk)
        source = Path('/mnt/ibl').joinpath(rel_path)
        dest = Path('/mnt/ibl/public').joinpath(rel_path)
        if source.exists():
            if dest.exists():
                print(f'...destination exists: {dest}')
            else:
                dest.parent.mkdir(exist_ok=True, parents=True)
                dest.symlink_to(source)
        else:
            print(f'...source does not exist: {source}')

print("Finished creating symlinks\n")



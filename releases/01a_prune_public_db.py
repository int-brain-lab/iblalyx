"""
This script is designed to run using a Django installation that has access to the public alyx database.
It can be run on the SDSC server or locally, as long as the 'public' database is connected to the openalyx RDS instance.
Make sure to set the openalyx website to maintenance (see dev playbook)!

This script is part of a shell script sequence in which the public database is first recreated as a copy of the production
database. Subsequently, this script will
- read a list of dataset IDs to be public, based on all previous data releases
- remove any information in the database that is NOT associated with these datasets
- remove confidential information and experimental information that is not necessary for using the data

After this script, symlinks from the internal IBL disk to the publicly exposed folder need to be created on SDSC for
all datasets in the new public database. This public folder will then be synchronized with the AWS S3 public bucket.
"""


import json
import pandas as pd
from datetime import datetime
from pathlib import Path

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


"""
Settings and Inputs
"""
# This is an artefact of the PreReleaseAnnualMeeting, which did not specify which dataset types to release
# We remove the raw ones (most) for now, in the future any release should specify exactly which datasets to release
dtypes_exclude = DatasetType.objects.filter(name__icontains='raw').exclude(name__in=['_iblrig_Camera.raw',
                                                                                     '_iblrig_RFMapStim.raw'])

public_ds_files = ['2021_Q1_IBL_et_al_datasets.pqt',
                   '2021_Q2_Varol_et_al_datasets.pqt',
                   '2021_Q3_Whiteway_et_al_datasets.pqt',
                   '2021_Q2_PreRelease_datasets.pqt'
                   ]
public_ds_tags = [
    "cfc4906a-316e-4150-8222-fe7e7f13bdac",  # "Behaviour Paper", "2021_Q1_IBL_et_al"
    "9dec1de8-389d-40f6-b00b-763e4fda6552",  # "Erdem's paper", "2021_Q2_Varol_et_al"
    "c8f0892a-a95b-4181-b8e6-d5d31cb97449",  # "Matt's paper", "2021_Q3_Whiteway_et_al"
    "dcd8b2e5-3a32-41b4-ac15-085a208a4466",  # "May 2021 pre-release", "2021_Q2_PreRelease"
    ]

# Get public aws information from local file to avoid storing this on github,
# This should be stored in the home directory of the user
aws_info_file = Path.home().joinpath('aws_public_info.json')
with open(aws_info_file) as f:
    aws_info = json.load(f)

print(f"Dataset IDs from files: {public_ds_files}")
print(f"Dataset types to exclude: {list(dtypes_exclude.values_list('name', flat=True))}")
print(f"Tags to keep: {public_ds_tags}\n")

# Load all datasets ids into one list
# TECHNICAL DEBT: This is probably not great for the future when we start to have many more datasets public
public_ds_ids = []
for f in public_ds_files:
    public_ds_ids.extend(list(pd.read_parquet(f)['dataset_id']))
public_ds_ids = list(set(public_ds_ids))

"""
Pruning and anonymizing database
"""
print(f"\nStarting to prune public database")
print("...pruning datasets")
# Delete all datasets that are not in that list, along with their file records, also datasets with to be excluded types
Dataset.objects.using('public').filter(dataset_type__in=dtypes_exclude).delete()
Dataset.objects.using('public').exclude(pk__in=public_ds_ids).delete()
datasets = Dataset.objects.using('public').all()

# Delete tags that aren't in the list above (released datasets might have additional, not-yet-released tags)
Tag.objects.using('public').exclude(pk__in=public_ds_tags).delete()

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
DatasetType.objects.using('public').exclude(pk__in=datasets.values_list('dataset_type', flat=True).distinct()).delete()
DataFormat.objects.using('public').exclude(pk__in=datasets.values_list('data_format', flat=True).distinct()).delete()
Revision.objects.using('public').exclude(pk__in=datasets.values_list('revision', flat=True).distinct()).delete()

print("...pruning sessions")
# Delete sessions that don't have a dataset in public db, along with probe insertions, trajectories, channels and tasks
Session.objects.using('public').exclude(id__in=datasets.values_list('session_id', flat=True)).delete()
sessions = Session.objects.using('public').all()
# Remove the session json and narrative which contain identifying information
sessions.update(json=None, narrative='')

print("...pruning subjects")
# Delete all subjects that don't have a session along with many actions
Subject.objects.using('public').exclude(actions_sessions__in=sessions).delete()
subjects = Subject.objects.using('public').all()
# Remove subject json
subjects.update(json=None, description='', death_date=None)

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



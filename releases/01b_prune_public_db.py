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

import os
import sys
from datetime import datetime
from pathlib import Path

import django

if __name__ == '__main__' and not os.environ.get('DJANGO_SETTINGS_MODULE'):
    sys.path.insert(0, '.')
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'alyx.settings')
    django.setup()

import pandas as pd
import tqdm
from django.db.backends.signals import connection_created
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

# iblalyx is bind-mounted whole into the container at /home/iblalyx (see docker-compose.yaml).
IBLALYX_ROOT = Path('/home/iblalyx')
sys.path.insert(0, str(IBLALYX_ROOT.parent))
import iblalyx.releases.utils


def _enforce_default_read_only(sender, connection, **kwargs):
    """Safeguard: everything below should use('public'). If a typo omits that and a query
    hits 'default' (production) instead, fail loudly rather than silently writing to production."""
    if connection.alias == 'default':
        with connection.cursor() as cursor:
            cursor.execute('SET default_transaction_read_only = ON;')


connection_created.connect(_enforce_default_read_only)


def delete_in_batches(queryset, model, batch_size=100000, **delete_kwargs):
    """Delete a queryset in batches of pks, each batch its own transaction.

    A single delete() on a queryset this large can cascade to millions of related rows (e.g.
    Session -> ProbeInsertion -> TrajectoryEstimate -> Channel) in one transaction, which has
    previously grown Postgres's WAL/memory enough to crash the host outright. In practice memory
    stayed flat well under the container's limit at batch_size=20000, so the batch size here is
    sized for transaction/commit overhead, not memory.
    """
    ids = list(queryset.values_list('pk', flat=True).iterator(chunk_size=50000))
    n = len(ids)
    for i in tqdm.tqdm(range(0, n, batch_size), desc=f'Deleting {model.__name__} ({n} rows)'):
        batch = ids[i:i + batch_size]
        model.objects.using(queryset.db).filter(pk__in=batch).delete(**delete_kwargs)
    return n


public_ds_files = iblalyx.releases.utils.PUBLIC_DS_FILES
public_ds_tags = iblalyx.releases.utils.PUBLIC_DS_TAGS

# Check that files exist
for f in public_ds_files:
    assert IBLALYX_ROOT.joinpath('releases', f).exists()

print(f"Dataset IDs from files: {public_ds_files}")
print(f"Tags to keep: {public_ds_tags}\n")

"""
Pruning and anonymizing database
"""

print(f"\nStarting to prune public database")
# Delete all datasets that are not in that list, along with their file records
# TECHNICAL DEBT: This is probably not great for the future when we start to have many more datasets public
print("...pruning datasets")
datasets_to_del = Dataset.objects.using('public').all()
for fname, tag_id in zip(public_ds_files, public_ds_tags):
    dataset_ids = pd.read_parquet(IBLALYX_ROOT.joinpath('releases', fname))['dataset_id']
    tag = Tag.objects.using('public').filter(id=tag_id)
    if len(tag) != 0:
        datasets = Dataset.objects.using('public').filter(pk__in=list(dataset_ids))
        if set(tag[0].datasets.values_list('pk', flat=True)) != set(datasets.values_list('pk', flat=True)):
            print(f"{fname} mismatched tags !")
    else:
        print(f"{fname} missing tag !")
    datasets_to_del = datasets_to_del.exclude(pk__in=list(dataset_ids))

# Now delete datasets (cascades to millions of FileRecords - see delete_in_batches above)
delete_in_batches(datasets_to_del, Dataset, force=True)
datasets = Dataset.objects.using('public').all()

# Delete tags that aren't in the list above (released datasets might have additional, not-yet-released tags)
# Tag.objects.using('public').exclude(pk__in=public_ds_tags).delete()

# Delete personal data repositories and associated file records
DataRepository.objects.using('public').exclude(globus_is_personal=False).delete()
DataRepository.objects.using('public').filter(name='ibl-brain-wide-map-private').delete()
# Replace some information in the data repositories
exclude = ('Secret access key', 'Access key ID')
for dr in DataRepository.objects.using('public').all():
    if 'flatiron' in dr.hostname:
        dr.data_url = dr.data_url.replace('.org/', '.org/public/')
        dr.json = {}
        dr.save()
    elif 'aws' in dr.hostname:
        dr.json = {k: v for k, v in (dr.json or {}).items() if k not in exclude}
        dr.json['bucket_name'] = 'ibl-brain-wide-map-public'
        dr.data_url = dr.data_url.replace('ibl-brain-wide-map-private', 'ibl-brain-wide-map-public')
        dr.save()

# Remove unused dataset types, formats and revisions
DatasetType.objects.using('public').exclude(pk__in=datasets.values_list('dataset_type', flat=True).distinct()).delete()
DataFormat.objects.using('public').exclude(pk__in=datasets.values_list('data_format', flat=True).distinct()).delete()
Revision.objects.using('public').exclude(pk__in=datasets.values_list('revision', flat=True).distinct()).delete()

print("...pruning sessions")
# Delete sessions that don't have a dataset in public db, along with probe insertions, trajectories, channels and tasks
session_ids = datasets.exclude(session__isnull=True).values_list('session_id', flat=True).distinct()
# Session -> ProbeInsertion -> TrajectoryEstimate -> Channel is a 3-level CASCADE chain reaching
# the ~3.76M-row Channel table, so even a modest number of sessions per batch can cascade to a
# huge number of rows - much smaller batches here than for the other, shallower-cascade deletes.
delete_in_batches(Session.objects.using('public').exclude(pk__in=session_ids), Session, batch_size=1000)
sessions = Session.objects.using('public').all()
# Remove the session json and narrative which contain identifying information
sessions.update(json=None, narrative='')

print("...pruning subjects")
# Delete all subjects that don't have a session OR A DATASET along with many actions
# This is a bit of a crutch for now, on the long run we should be able to query the content objects of non-session
# data more systematically
subs_to_keep = datasets.filter(
    session__isnull=True, object_id__isnull=False).values_list('object_id', flat=True)
delete_in_batches(
    Subject.objects.using('public').exclude(actions_sessions__in=sessions).exclude(pk__in=subs_to_keep),
    Subject)
subjects = Subject.objects.using('public').all()
# Remove subject json
subjects.update(json=None, description='', death_date=None)

print("...pruning projects")
# Delete projects that aren't attached to public session or subject
projects = Project.objects.using('public').filter(
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

# Anonymize remaining lab members and build dict for replace names elsewhere
anon_dict = {}
for lm in lab_members:
    if lm.username == 'root':
        continue
    anon_dict[lm.username] = str(lm.id)[:8]
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
                                                       is_staff=True,
                                                       is_public_user=True)
public_user.set_password('international')
public_user.groups.add(Group.objects.using('public').filter(name='Lab members')[0])
public_user.save()

print("...pruning probe insertions")
# Delete all remaining probe insertions that don't have datasets in the public database
probeinsertions = ProbeInsertion.objects.using('public').filter(
    datasets__pk__in=datasets.values_list('pk', flat=True)).distinct()
delete_in_batches(
    ProbeInsertion.objects.using('public').exclude(pk__in=probeinsertions.values_list('pk', flat=True)),
    ProbeInsertion, batch_size=2000)
# Remove identifying information from probe insertion json
probeinsertions = ProbeInsertion.objects.using('public').all()
for p in probeinsertions.iterator(chunk_size=2000):
    pdict = p.json
    if pdict and 'extended_qc' in pdict.keys() and 'alignment_stored' in pdict['extended_qc'].keys():
        datestr = pdict['extended_qc']['alignment_stored'][:20]
        exp_str = pdict['extended_qc']['alignment_stored'][20:]
        if exp_str in anon_dict.keys():
            pdict['extended_qc']['alignment_stored'] = datestr + anon_dict[exp_str]
        else:
            pdict['extended_qc']['alignment_stored'] = datestr + 'xxx'
        ProbeInsertion.objects.using('public').filter(pk=p.id).update(json=pdict)

# Delete all trajectories that are not associated with the probe insertions
delete_in_batches(
    TrajectoryEstimate.objects.using('public').exclude(probe_insertion__in=probeinsertions),
    TrajectoryEstimate, batch_size=2000)
# Remove identifying information from ephys aligned trajectory json
trajectories = TrajectoryEstimate.objects.using('public').filter(
    probe_insertion__in=probeinsertions).filter(provenance=70).distinct()
for t in trajectories.iterator(chunk_size=2000):
    tdict = t.json
    tdict_anon = {}
    if tdict:
        for key, val in tdict.items():
            datestr = key[:20]
            exp_str = key[20:]
            if exp_str in anon_dict.keys():
                anon_key = datestr + anon_dict[exp_str]
            else:
                anon_key = datestr + 'xxx'
            tdict_anon[anon_key] = val
        TrajectoryEstimate.objects.using('public').filter(pk=t.id).update(json=tdict_anon)

# Remove unused Probe models, coordinate systems, channels and Brain Regions
ProbeModel.objects.using('public').exclude(pk__in=probeinsertions.values_list('model', flat=True).distinct()).delete()
CoordinateSystem.objects.using('public').exclude(
    pk__in=trajectories.values_list('coordinate_system', flat=True)).delete()
delete_in_batches(Channel.objects.using('public').exclude(trajectory_estimate__in=trajectories), Channel)

print("...pruning cage, weight, water and food info")
# Get subjects and sessions in new open database
subjects = Subject.objects.using('public').all()
sessions = Session.objects.using('public').all()
# Keep Weighing and WaterRestriction only from those subjects
delete_in_batches(Weighing.objects.using('public').exclude(subject__in=subjects), Weighing)
WaterRestriction.objects.using('public').exclude(subject__in=subjects).delete()
# Keep WaterAdministration only from those sessions
delete_in_batches(
    WaterAdministration.objects.using('public').exclude(session__in=sessions, session__isnull=False),
    WaterAdministration)
# From the HousinSubject through table, remove all entries that are not related to a subject
HousingSubject.objects.using('public').exclude(subject__in=subjects).delete()
housingsubject = HousingSubject.objects.using('public').all()
# Remove all Housing entries that no longer have an entry in HousingSubject
Housing.objects.using('public').exclude(id__in=housingsubject.values_list('housing', flat=True)).delete()
housing = Housing.objects.using('public').all()
# Remove all CageType, Enrichment and Food entries that are not related to a Housing in the new public DB
CageType.objects.using('public').exclude(housing__in=housing).delete()
Enrichment.objects.using('public').exclude(housing__in=housing).delete()
Food.objects.using('public').exclude(housing__in=housing).delete()


"""
Deleting some tables altogether
"""
print("...deleting some tables")
# misc
LabMembership.objects.using('public').all().delete()
LabLocation.objects.using('public').all().delete()
Note.objects.using('public').all().delete()

# actions
ProcedureType.objects.using('public').all().delete()
VirusInjection.objects.using('public').all().delete()
Surgery.objects.using('public').all().delete()
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

"""Carry openalyx's own accounts across a data release.

A release rebuilds openalyx from a copy of production (01a), prunes it (01b) and swaps the
result in for the live database (02). That swap replaces the user table wholesale, so any
account that exists only on openalyx - a member of the public who registered at /signup or
signed in through ORCID, the shared intbrainlab login - would be lost with it.

This script exports those accounts from the live database before the swap and imports them into
the staged one. It is invoked by 02_upload_public_db.sh; run it by hand only to inspect a
release or to recover from a failed one.

Accounts to preserve are those with is_public_user set, together with everything that makes one
usable: the API token ONE authenticates with, and - where single sign-on is enabled - the
allauth records that tie the account to an ORCID iD. Those identity records matter more than
they look: ORCID supplies no email address, so a lost SocialAccount row cannot be re-matched by
any other means, and allauth would treat the next sign-in as a new person and create a second
account.

The anonymised lab members that 01b carries over from production are not preserved - every
release recreates them - and neither are administrator accounts, which are provisioned on the
instance rather than carried across.

    # export from the live database
    python public_accounts.py export --output /path/accounts.json

    # import into the staged database (OPENALYX_DB_NAME points the alias at it)
    python public_accounts.py import --input /path/accounts.json

The file is written with Django's own serializer, so `manage.py loaddata` can read it if this
script is ever unavailable. It holds email addresses, password hashes and API tokens: treat it
as a credential file, keep it private, and delete it once the release is confirmed.
"""

import argparse
import json
import os
import sys

import django

if __name__ == '__main__':
    # setdefault already leaves an existing DJANGO_SETTINGS_MODULE alone, so there is no need
    # to guard on it - and guarding on it means that where the environment does define one
    # (as a container running alyx generally will) django.setup() never runs and every model
    # import below fails with AppRegistryNotReady.
    sys.path.insert(0, '.')
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'alyx.settings')
    django.setup()

from django.apps import apps  # noqa: E402
from django.contrib.auth.models import Group  # noqa: E402
from django.core import serializers  # noqa: E402
from django.core.serializers.json import DjangoJSONEncoder  # noqa: E402
from django.core.management.color import no_style  # noqa: E402
from django.db import connections, transaction  # noqa: E402

from misc.models import LabMember  # noqa: E402

DATABASE = 'openalyx'

# Everything belonging to a preserved account, in the order it must be restored: the user first,
# then the rows that point at it. Each entry is (label, filter kwargs relative to the user).
# Optional models are skipped where their app is not installed, so this works the same whether
# or not the deployment has single sign-on.
RELATED_MODELS = (
    ('authtoken.Token', 'user'),
    ('socialaccount.SocialAccount', 'user'),
    ('account.EmailAddress', 'user'),
)


def _related_querysets(database, users):
    """Yield (label, queryset) for each related model that exists in this installation."""
    for label, user_field in RELATED_MODELS:
        try:
            model = apps.get_model(label)
        except LookupError:
            continue  # app not installed; nothing of this kind to carry
        yield label, model.objects.using(database).filter(**{f'{user_field}__in': users})


def export_accounts(output, database=DATABASE):
    users = LabMember.objects.using(database).filter(is_public_user=True).order_by('username')
    counts = {'misc.LabMember': users.count()}

    # Natural foreign keys are used for the user records and no further, which is deliberate.
    # Group membership must travel by name: group primary keys come from whichever copy of
    # production the database was built from and can shift between releases. Everything else
    # must travel by primary key: a natural key is resolved by querying the target database at
    # deserialization time, so a Token whose user were written as ["alovelace"] could not be
    # read back until that user already existed - and, worse, would silently attach itself to
    # a different account that happened to hold the name.
    data = serializers.serialize('python', users, use_natural_foreign_keys=True)
    for label, queryset in _related_querysets(database, users):
        counts[label] = queryset.count()
        data += serializers.serialize('python', queryset, use_natural_foreign_keys=False)

    with open(output, 'w') as fp:
        json.dump(data, fp, indent=1, cls=DjangoJSONEncoder)
    os.chmod(output, 0o600)

    print(f'Exported {len(data)} record(s) from "{database}" to {output}')
    for label, count in counts.items():
        print(f'  {count:5d}  {label}')
    for user in users:
        print(f'         {user.username}'
              f'{"" if user.is_active else " (inactive)"}'
              f'{"" if user.email else " (no email)"}')
    if not counts['misc.LabMember']:
        print('WARNING: no public accounts found. Expected at least intbrainlab - check that '
              'this is pointing at the live openalyx database.')
    return data


def _reset_sequences(database, models):
    """Realign auto-increment sequences with the primary keys just inserted.

    Restoring rows with explicit primary keys leaves a table's sequence where it was, so the
    next insert - the next person to sign in through ORCID - would collide with a restored row.
    `loaddata` does this for itself; this script restores selectively, so it does it here.
    """
    connection = connections[database]
    statements = connection.ops.sequence_reset_sql(no_style(), list(models))
    if not statements:
        return
    with connection.cursor() as cursor:
        for statement in statements:
            cursor.execute(statement)


def _ensure_groups(raw, database, dry_run):
    """Make sure every group the fixture references exists in the target database.

    Group membership is serialized by name, and deserialization raises - aborting the whole
    restore - if a name has no match. That would be a poor way to fail: it happens after the
    swap, with the site in maintenance, and would leave openalyx with no public accounts at
    all. Missing groups are therefore created empty and reported, so the release completes and
    the permissions can be repaired afterwards without anyone having lost their account.

    In practice the groups come across in the copy of production, and 01b refuses to build a
    release without 'Public users', so this should never fire.
    """
    referenced = set()
    for record in raw:
        for name in record.get('fields', {}).get('groups', []) or []:
            # Natural keys are serialized as a list of the key's parts; Group's is (name,).
            referenced.add(name[0] if isinstance(name, (list, tuple)) else name)
    if not referenced:
        return
    present = set(Group.objects.using(database).filter(
        name__in=referenced).values_list('name', flat=True))
    for name in sorted(referenced - present):
        print(f'  WARNING: group {name!r} does not exist in "{database}". '
              f'{"Would create" if dry_run else "Creating"} it empty - run '
              f'`manage.py set_public_permissions` on production so that it survives pruning.')
        if not dry_run:
            Group.objects.using(database).create(name=name)


def import_accounts(input_file, database=DATABASE, dry_run=False):
    with open(input_file) as fp:
        raw = json.load(fp)

    # Never overwrite a row that arrived with the release. 01b renames every lab member it
    # anonymises to the first characters of its own UUID, so a clash with a real one is not
    # realistically possible - but intbrainlab is recreated by 01b with a fresh UUID, and
    # silently replacing an account is not something to do without saying so.
    taken = dict(LabMember.objects.using(database).values_list('username', 'pk'))
    skipped = {}  # pk of skipped user -> username
    for record in raw:
        if record['model'] != 'misc.labmember':
            continue
        username = record['fields']['username']
        existing = taken.get(username)
        if existing is not None and str(existing) != str(record['pk']):
            skipped[str(record['pk'])] = username

    keep = [record for record in raw
            if str(record.get('pk')) not in skipped
            # a row belonging to a skipped account would otherwise attach itself to the
            # release's account of the same name
            and str(record['fields'].get('user', '')) not in skipped]

    if dry_run:
        print(f'Would restore {len(keep)} record(s) into "{database}"')
        _ensure_groups(raw, database, dry_run=True)
        for username in sorted(skipped.values()):
            print(f'  COLLISION: {username} already exists in the release, would be skipped')
        return keep

    _ensure_groups(raw, database, dry_run=False)
    models = set()
    with transaction.atomic(using=database):
        for item in serializers.deserialize('json', json.dumps(keep), using=database):
            item.save(using=database)
            models.add(type(item.object))
    _reset_sequences(database, models)

    print(f'Restored {len(keep)} record(s) into "{database}"')
    for model in sorted(models, key=lambda m: m._meta.label):
        print(f'  {model._meta.label}')
    for username in sorted(skipped.values()):
        print(f'  SKIPPED: {username} already exists in the release (not overwritten)')
    return keep


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--database', default=DATABASE,
                        help=f'Django database alias to act on (default: {DATABASE})')
    sub = parser.add_subparsers(dest='action', required=True)

    export = sub.add_parser('export', help='write the public accounts to a JSON fixture')
    export.add_argument('-o', '--output', required=True)

    restore = sub.add_parser('import', help='read accounts back from a JSON fixture')
    restore.add_argument('-i', '--input', required=True)
    restore.add_argument('--dry-run', action='store_true',
                         help='report what would be restored, and any username collisions, '
                              'without writing anything')

    args = parser.parse_args()
    if args.action == 'export':
        export_accounts(args.output, database=args.database)
    else:
        import_accounts(args.input, database=args.database, dry_run=args.dry_run)


if __name__ == '__main__':
    main()

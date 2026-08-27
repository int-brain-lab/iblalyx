"""Carry openalyx's own accounts across a data release.

A release rebuilds openalyx from a copy of production (01a), prunes it (01b) and swaps the
result in for the live database (02). That swap replaces the user table wholesale, so any
account that exists only on openalyx - a member of the public who registered at /signup, the
shared intbrainlab login, an administrator - would be lost with it.

This script exports those accounts from the live database before the swap and imports them into
the staged one, so that registrations survive. It is invoked by 02_upload_public_db.sh; run it
by hand only to inspect a release or to recover from a failed one.

Accounts to preserve are those with is_public_user set: the members of the public who
registered at /signup, and the shared intbrainlab login. The anonymised lab members that 01b
carries over from production are not preserved, because every release recreates them from
production anyway. Neither are administrator accounts, which are provisioned on the instance
rather than carried across.

    # export from the live database
    python public_accounts.py export --output /path/accounts.json

    # import into the staged database (OPENALYX_DB_NAME points the alias at it)
    python public_accounts.py import --input /path/accounts.json

The JSON holds email addresses, password hashes and API tokens. It is a credential file: keep
it private and delete it once the release is confirmed.
"""

import argparse
import json
import os
import sys

import django

if __name__ == '__main__' and not os.environ.get('DJANGO_SETTINGS_MODULE'):
    sys.path.insert(0, '.')
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'alyx.settings')
    django.setup()

from django.contrib.auth.models import Group  # noqa: E402
from django.db import transaction  # noqa: E402
from rest_framework.authtoken.models import Token  # noqa: E402

from misc.models import LabMember  # noqa: E402

DATABASE = 'openalyx'

# Fields carried across a release. password is already a hash, never plain text. The API token
# comes too, so that a release does not silently break every public user's ONE installation.
USER_FIELDS = (
    'id', 'username', 'email', 'password', 'first_name', 'last_name',
    'is_active', 'is_staff', 'is_superuser', 'is_public_user', 'is_stock_manager',
    'date_joined', 'last_login',
)


def export_accounts(output, database=DATABASE):
    users = LabMember.objects.using(database).filter(is_public_user=True).order_by('username')

    accounts = []
    for user in users:
        record = {field: getattr(user, field) for field in USER_FIELDS}
        record['id'] = str(record['id'])
        for field in ('date_joined', 'last_login'):
            record[field] = record[field].isoformat() if record[field] else None
        record['groups'] = sorted(user.groups.values_list('name', flat=True))
        token = Token.objects.using(database).filter(user=user).first()
        record['token'] = token.key if token else None
        accounts.append(record)

    with open(output, 'w') as fp:
        json.dump({'version': 1, 'accounts': accounts}, fp, indent=1)
    os.chmod(output, 0o600)

    print(f'Exported {len(accounts)} account(s) from "{database}" to {output}')
    for record in accounts:
        print(f'  {record["username"]}'
              f'{" (inactive)" if not record["is_active"] else ""}'
              f'{" (superuser)" if record["is_superuser"] else ""}')
    if not accounts:
        print('WARNING: no public accounts found. Expected at least intbrainlab - check that '
              'this is pointing at the live openalyx database.')
    return accounts


def import_accounts(input_file, database=DATABASE, dry_run=False):
    with open(input_file) as fp:
        payload = json.load(fp)
    if payload.get('version') != 1:
        raise SystemExit(f'Unsupported export format version {payload.get("version")!r}')
    accounts = payload['accounts']

    existing = set(LabMember.objects.using(database).values_list('username', flat=True))
    restored, skipped = [], []

    for record in accounts:
        # Never overwrite a row that arrived with the release. A username that used to belong to
        # a registered public user can be taken by a real lab member appearing in production
        # later, and that lab member's row is the one the released data is attributed to.
        if record['username'] in existing:
            skipped.append(record['username'])
            continue
        restored.append(record)

    if dry_run:
        print(f'Would restore {len(restored)} account(s) into "{database}"')
        for username in skipped:
            print(f'  COLLISION: {username} already exists in the release, would be skipped')
        return restored

    with transaction.atomic(using=database):
        groups = {group.name: group for group in Group.objects.using(database).all()}
        for record in restored:
            fields = {field: record[field] for field in USER_FIELDS}
            user = LabMember(**fields)
            user.save(using=database)
            names = [name for name in record['groups'] if name in groups]
            if missing := set(record['groups']) - set(groups):
                print(f'  WARNING: {record["username"]} was in group(s) '
                      f'{", ".join(sorted(missing))}, which do not exist in the release. '
                      f'Run set_public_permissions on production so the group survives pruning.')
            if names:
                user.groups.add(*[groups[name] for name in names])
            if record['token']:
                Token.objects.using(database).create(user=user, key=record['token'])

    print(f'Restored {len(restored)} account(s) into "{database}"')
    for username in skipped:
        print(f'  SKIPPED: {username} already exists in the release (not overwritten)')
    return restored


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--database', default=DATABASE,
                        help=f'Django database alias to act on (default: {DATABASE})')
    sub = parser.add_subparsers(dest='action', required=True)

    export = sub.add_parser('export', help='write local accounts to a JSON file')
    export.add_argument('-o', '--output', required=True)

    restore = sub.add_parser('import', help='read accounts back from a JSON file')
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

"""Report on, compact and expire the client usage records.

    manage.py client_usage report [--days 30] [--account intbrainlab]
    manage.py client_usage purge [--keep-days 400]

`purge` is meant to run daily: it turns each ended day's shared-account fingerprints into a
count of distinct callers and deletes them, deletes records older than --keep-days, and records
the signups of each ended day not yet recorded (all of them, the first time it runs).
"""
import datetime
from collections import defaultdict

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Count, Sum
from django.db.models.functions import TruncDate

from client_usage.middleware import flush
from client_usage.models import ClientFingerprint, ClientUsage, Signups
from client_usage.router import usage_database


class Command(BaseCommand):
    help = 'Report on, compact and expire client usage records.'

    def add_arguments(self, parser):
        sub = parser.add_subparsers(dest='action', required=True)
        report = sub.add_parser('report', help='summarise usage over recent days')
        report.add_argument('--days', type=int, default=30)
        report.add_argument('--account', help='only this username')
        purge = sub.add_parser(
            'purge', help='compact ended days, expire old records and record signups')
        purge.add_argument('--keep-days', type=int,
                           default=getattr(settings, 'CLIENT_USAGE_RETENTION_DAYS', 400))

    def handle(self, *args, action=None, **options):
        flush()  # include this process's own buffered counts, if any
        if action == 'report':
            self.report(options['days'], options.get('account'))
        else:
            self.purge(options['keep_days'])

    def report(self, days, account=None):
        since = datetime.date.today() - datetime.timedelta(days=days - 1)
        usage = ClientUsage.objects.using(usage_database()).filter(date__gte=since)
        if account:
            usage = usage.filter(username=account)
        self.stdout.write(f'Since {since} ({days} days){f" for {account}" if account else ""}')
        rows = (usage.values('client', 'version')
                .annotate(accounts=Count('user_id', distinct=True), requests=Sum('requests'))
                .order_by('client', '-requests'))
        self.stdout.write(f'{"client":<16} {"version":<12} {"accounts":>8} {"requests":>10}')
        for r in rows:
            self.stdout.write(f'{r["client"]:<16} {r["version"] or "-":<12} '
                              f'{r["accounts"]:>8} {r["requests"]:>10}')

        shared = getattr(settings, 'CLIENT_USAGE_SHARED_ACCOUNTS', ())
        for username in shared if not account else [a for a in shared if a == account]:
            callers = self._distinct_callers(usage.filter(username=username), since)
            if not callers:
                continue
            self.stdout.write(f'\nDistinct callers per day of shared account {username}')
            self.stdout.write(f'{"client":<16} {"version":<12} {"days":>5} {"mean":>6} {"max":>5}')
            for (client, version), per_day in sorted(callers.items()):
                counts = list(per_day.values())
                self.stdout.write(f'{client:<16} {version or "-":<12} {len(counts):>5} '
                                  f'{sum(counts) / len(counts):>6.1f} {max(counts):>5}')

        if not account:
            signups = (Signups.objects.using(usage_database()).filter(date__gte=since)
                       .values('public').annotate(n=Sum('accounts')))
            totals = {r['public']: r['n'] for r in signups}
            self.stdout.write(f'\nSignups (ended days only): {totals.get(True, 0)} public, '
                              f'{totals.get(False, 0)} other')

    @staticmethod
    def _distinct_callers(usage, since):
        """{(client, version): {date: callers}} from compacted days plus unended ones."""
        callers = defaultdict(dict)
        for r in usage.exclude(distinct_clients=None):
            callers[(r.client, r.version)][r.date] = r.distinct_clients
        live = (ClientFingerprint.objects.using(usage_database())
                .filter(user_id__in=usage.values_list('user_id', flat=True), date__gte=since)
                .values('date', 'client', 'version').annotate(n=Count('fingerprint')))
        for r in live:
            callers[(r['client'], r['version'])][r['date']] = r['n']
        return callers

    def purge(self, keep_days):
        db = usage_database()
        today = datetime.date.today()
        ended = ClientFingerprint.objects.using(db).filter(date__lt=today)
        groups = ended.values('date', 'user_id', 'client', 'version').annotate(
            n=Count('fingerprint'))
        with transaction.atomic(using=db):
            for g in groups:
                ClientUsage.objects.using(db).filter(
                    date=g['date'], user_id=g['user_id'], client=g['client'],
                    version=g['version']).update(distinct_clients=g['n'])
            compacted = ended.delete()[0]
            cutoff = today - datetime.timedelta(days=keep_days)
            expired = ClientUsage.objects.using(db).filter(date__lt=cutoff).delete()[0]
        self.stdout.write(f'Compacted {compacted} fingerprint(s) into {len(groups)} count(s); '
                          f'deleted {expired} record(s) from before {cutoff}.')
        self.record_signups(today)

    def record_signups(self, today):
        """Count the accounts that joined on each ended day not yet recorded."""
        db = usage_database()
        last = Signups.objects.using(db).order_by('-date').values_list('date', flat=True).first()
        accounts = get_user_model().objects.exclude(
            username__in=getattr(settings, 'CLIENT_USAGE_SHARED_ACCOUNTS', ()))
        if last is None:  # first run: from the earliest account
            first = accounts.order_by('date_joined').values_list('date_joined', flat=True).first()
            if first is None:
                return
            start = first.date()
        else:
            start = last + datetime.timedelta(days=1)
        if start >= today:
            return
        joined = (accounts.filter(date_joined__date__gte=start, date_joined__date__lt=today)
                  .annotate(day=TruncDate('date_joined')).values('day', 'is_public_user')
                  .annotate(n=Count('pk')))
        counts = {(r['day'], r['is_public_user']): r['n'] for r in joined}
        # Every day gets a row, zero or not, so a chart has no gaps and the last day recorded is
        # the last day counted.
        days = [start + datetime.timedelta(days=i) for i in range((today - start).days)]
        Signups.objects.using(db).bulk_create(
            [Signups(date=day, public=public, accounts=counts.get((day, public), 0))
             for day in days for public in (True, False)],
            ignore_conflicts=True)
        self.stdout.write(f'Recorded signups for {len(days)} day(s), {start} to {days[-1]}: '
                          f'{sum(counts.values())} account(s).')

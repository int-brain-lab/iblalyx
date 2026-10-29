"""Daily aggregates of which client software each account uses.

Accounts are recorded by id and name rather than by foreign key, because this table may live in
a database of its own (see router.py), which a foreign key cannot reach.
"""
from django.db import models


class ClientUsage(models.Model):
    """Requests made by one account with one client version on one day."""

    date = models.DateField()
    user_id = models.UUIDField()
    username = models.CharField(max_length=150)
    client = models.CharField(max_length=32)
    version = models.CharField(max_length=32, blank=True)
    requests = models.PositiveIntegerField(default=0)
    #: Distinct callers behind a shared account, once its day is compacted; None otherwise.
    distinct_clients = models.PositiveIntegerField(null=True, blank=True)

    class Meta:
        constraints = [models.UniqueConstraint(
            fields=('date', 'user_id', 'client', 'version'), name='client_usage_unique_day')]
        indexes = [models.Index(fields=('date', 'client', 'version'))]

    def __str__(self):
        return f'{self.date} {self.username} {self.client} {self.version}: {self.requests}'


class ClientFingerprint(models.Model):
    """One distinct caller of a shared account, for the current day only.

    The fingerprint is a keyed hash of the caller's address and User-Agent, keyed per day, so
    that it counts callers without storing addresses or linking a caller across days. Days that
    have ended are compacted into ClientUsage.distinct_clients and their rows deleted by
    `manage.py client_usage purge`; past fingerprints are never kept.
    """

    date = models.DateField()
    user_id = models.UUIDField()
    client = models.CharField(max_length=32)
    version = models.CharField(max_length=32, blank=True)
    fingerprint = models.CharField(max_length=64)

    class Meta:
        constraints = [models.UniqueConstraint(
            fields=('date', 'user_id', 'client', 'version', 'fingerprint'),
            name='client_fingerprint_unique')]


class Signups(models.Model):
    """Accounts that joined on one day, counted once the day has ended.

    Recorded by `manage.py client_usage purge` from the accounts' join dates, and never revised:
    an account deleted later still counts on the day it joined. Shared accounts are left out,
    as a data release recreates them with a new join date.
    """

    date = models.DateField()
    #: Self-registered public accounts, as opposed to accounts made by an administrator.
    public = models.BooleanField()
    accounts = models.PositiveIntegerField()

    class Meta:
        constraints = [models.UniqueConstraint(
            fields=('date', 'public'), name='client_usage_signups_unique_day')]

    def __str__(self):
        return f'{self.date} {"public" if self.public else "other"}: {self.accounts}'

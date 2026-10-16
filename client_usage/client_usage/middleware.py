"""Count authenticated requests by account, day and client, and pass notices to accounts.

Plain Django middleware is enough even for token-authenticated REST requests: DRF sets the user
it authenticates on the underlying Django request, so it is visible here once the response has
been produced.

Counts are buffered in each process and written at most every CLIENT_USAGE_FLUSH_SECONDS, so a
request costs a dictionary update rather than a database write. A process that dies loses at
most that interval's counts.
"""
import atexit
import datetime
import hashlib
import hmac
import logging
import threading
import time
from collections import Counter

from django.conf import settings
from django.db import connections, transaction

from .classify import classify
from .models import ClientFingerprint, ClientUsage
from .router import usage_database

logger = logging.getLogger(__name__)

#: Response header carrying notices to clients; the one Alyx uses for its own advice.
NOTICE_HEADER = 'Alyx-API-Notice'

_lock = threading.Lock()
_counts = Counter()  # (date, user_id, username, client, version) -> requests
_fingerprints = set()  # (date, user_id, client, version, fingerprint)
_last_flush = time.monotonic()


def _setting(name, default):
    return getattr(settings, name, default)


def fingerprint(day, request):
    """A per-day keyed hash of the caller's address and User-Agent.

    Keyed on SECRET_KEY and the date, so the same caller hashes differently on another day. It
    is only kept until its day is compacted (see ClientFingerprint).
    """
    key = f'{settings.SECRET_KEY}:client_usage:{day.isoformat()}'.encode()
    caller = f"{request.META.get('REMOTE_ADDR', '')}|{request.headers.get('User-Agent', '')}"
    return hmac.new(key, caller.encode(), hashlib.sha256).hexdigest()


def record(request, user):
    client, version = classify(request.headers)
    today = datetime.date.today()
    shared = user.username in _setting('CLIENT_USAGE_SHARED_ACCOUNTS', ())
    fp = fingerprint(today, request) if shared else None
    with _lock:
        _counts[(today, user.pk, user.username, client, version)] += 1
        if fp:
            _fingerprints.add((today, user.pk, client, version, fp))
        due = time.monotonic() - _last_flush >= _setting('CLIENT_USAGE_FLUSH_SECONDS', 60)
    if due:
        flush()


def flush():
    """Write the buffered counts. On failure they are kept for the next attempt."""
    global _last_flush
    with _lock:
        counts, fingerprints = dict(_counts), set(_fingerprints)
        _counts.clear()
        _fingerprints.clear()
        _last_flush = time.monotonic()
    if not counts and not fingerprints:
        return
    try:
        _write(counts, fingerprints)
    except Exception:
        logger.exception('Could not record client usage; will retry')
        with _lock:
            _counts.update(counts)
            _fingerprints.update(fingerprints)


def _write(counts, fingerprints):
    db = usage_database()
    usage = ClientUsage._meta.db_table
    fps = ClientFingerprint._meta.db_table
    with transaction.atomic(using=db), connections[db].cursor() as cursor:
        if counts:
            cursor.executemany(
                f'INSERT INTO {usage} (date, user_id, username, client, version, requests) '
                f'VALUES (%s, %s, %s, %s, %s, %s) '
                f'ON CONFLICT (date, user_id, client, version) DO UPDATE SET '
                f'requests = {usage}.requests + EXCLUDED.requests, username = EXCLUDED.username',
                [(*key, n) for key, n in counts.items()])
        if fingerprints:
            cursor.executemany(
                f'INSERT INTO {fps} (date, user_id, client, version, fingerprint) '
                f'VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING', list(fingerprints))


atexit.register(flush)


class ClientUsageMiddleware:

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        response = self.get_response(request)
        try:
            user = getattr(request, 'user', None)
            if user is not None and user.is_authenticated:
                if notice := _setting('CLIENT_USAGE_NOTICES', {}).get(user.username):
                    existing = response.get(NOTICE_HEADER)
                    response[NOTICE_HEADER] = f'{existing} {notice}' if existing else notice
                record(request, user)
        except Exception:
            # Usage accounting must never cost anyone their response.
            logger.exception('Client usage middleware failed')
        return response

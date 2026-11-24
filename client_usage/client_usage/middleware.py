"""Count authenticated requests by account, day and client, and pass notices to accounts.

Plain Django middleware is enough even for token-authenticated REST requests: DRF sets the user
it authenticates on the underlying Django request, so it is visible here once the response has
been produced.

Outdated ONE releases cannot display notices, so a fraction of their requests may instead be
refused with the notice as the error (CLIENT_USAGE_OUTDATED_RATE). That decision is made before
the view runs, so a refused request does no work.

Counts are buffered in each process and written at most every CLIENT_USAGE_FLUSH_SECONDS, so a
request costs a dictionary update rather than a database write. A process that dies loses at
most that interval's counts.
"""
import atexit
import datetime
import hashlib
import hmac
import logging
import random
import re
import threading
import time
from collections import Counter

from django.conf import settings
from django.core.cache import cache
from django.db import connections, transaction
from django.http import JsonResponse
from rest_framework.request import Request
from rest_framework.settings import api_settings

from .classify import classify
from .models import ClientFingerprint, ClientUsage
from .router import usage_database

logger = logging.getLogger(__name__)

#: Response header carrying notices to clients; the one Alyx uses for its own advice.
NOTICE_HEADER = 'Alyx-API-Notice'

#: Status of a request refused to tell an outdated client to update. Every ONE release raises a
#: failed request's `detail` as an HTTPError, except where it handles the status itself: 403
#: "Invalid token." (logs in again), 404 (in places taken to mean "not found") and, from 3.5, 429
#: and 503 (retries).
OUTDATED_STATUS = 426  # Upgrade Required

#: Paths never refused. Every ONE release swallows an error loading the cache tables, logging
#: only that it failed, and then sends every search to the database instead.
EXEMPT_PATHS = ('/cache/info', '/cache.zip')

#: Methods that may be refused; a write is never cut off part way through a registration.
REFUSABLE_METHODS = ('GET', 'HEAD')

DEFAULT_OUTDATED_MESSAGE = (
    'This version of ONE is out of date. Please update it with `pip install -U ONE-api`. '
    'Some requests from outdated versions are refused to bring this to your attention; '
    'repeating this one will usually succeed.')

_VERSION = re.compile(r'\d+(?:\.\d+)*')

_lock = threading.Lock()
_counts = Counter()  # (date, user_id, username, client, version) -> requests
_fingerprints = set()  # (date, user_id, client, version, fingerprint)
_last_flush = time.monotonic()


def _setting(name, default):
    return getattr(settings, name, default)


def _version(version):
    """A version string as a tuple of integers, e.g. (3, 6, 1), or None if it has none."""
    match = _VERSION.match(version or '')
    return tuple(map(int, match.group(0).split('.'))) if match else None


def is_outdated(headers):
    """Whether a request comes from a ONE release older than CLIENT_USAGE_MIN_ONE_VERSION.

    Releases before 3.6.0 send no version and identify themselves only by the requests library's
    User-Agent, so every such client is taken to be an outdated ONE. A ONE whose version cannot
    be read is not.
    """
    client, version = classify(headers)
    if client == 'python-requests':
        return True
    if client == 'ONE' and (version := _version(version)):
        return version < _version(_setting('CLIENT_USAGE_MIN_ONE_VERSION', '3.6.1'))
    return False


def authenticated_user(request):
    """The account making a request before its view has run, or None if it has none.

    Django's own middleware has already identified a browser session, but the view authenticates
    a token itself, so the request is put through the same DRF authentication classes here.
    """
    user = getattr(request, 'user', None)
    if user is not None and user.is_authenticated:
        return user
    authenticators = [auth() for auth in api_settings.DEFAULT_AUTHENTICATION_CLASSES]
    try:
        # On success this also sets the user on the Django request, as the view would.
        user = Request(request, authenticators=authenticators).user
    except Exception:  # e.g. an invalid or expired token
        return None
    return user if user.is_authenticated else None


def refusal(request):
    """A response refusing the request to tell an outdated client to update, or None.

    CLIENT_USAGE_OUTDATED_RATE of the authenticated GET and HEAD requests from outdated ONE
    releases are refused, other than those loading the cache tables, and each caller at most once
    per CLIENT_USAGE_OUTDATED_COOLDOWN seconds. The message is the account's entry in
    CLIENT_USAGE_OUTDATED_NOTICES, otherwise CLIENT_USAGE_OUTDATED_MESSAGE.
    """
    rate = _setting('CLIENT_USAGE_OUTDATED_RATE', 0)
    if (rate <= 0 or request.method not in REFUSABLE_METHODS
            or request.path_info.startswith(EXEMPT_PATHS)
            or not is_outdated(request.headers) or random.random() >= rate):
        return None
    if (user := authenticated_user(request)) is None:
        return None  # e.g. a login: ONE would report it as a failed one
    # A caller is an account at one address with one User-Agent, so the callers sharing an
    # account each have their own cooldown.
    caller = f'client_usage:refused:{user.pk}:{fingerprint(datetime.date.today(), request)}'
    if not cache.add(caller, True, _setting('CLIENT_USAGE_OUTDATED_COOLDOWN', 3600)):
        return None  # refused too recently
    message = (_setting('CLIENT_USAGE_OUTDATED_NOTICES', {}).get(user.username)
               or _setting('CLIENT_USAGE_OUTDATED_MESSAGE', DEFAULT_OUTDATED_MESSAGE))
    return JsonResponse({'detail': message}, status=OUTDATED_STATUS)


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
        try:
            refused = refusal(request)
        except Exception:
            # Telling a client to update must never cost it its response either.
            logger.exception('Client usage middleware failed')
            refused = None
        response = refused or self.get_response(request)
        try:
            user = getattr(request, 'user', None)
            if user is not None and user.is_authenticated:
                if notice := _setting('CLIENT_USAGE_NOTICES', {}).get(user.username):
                    if existing := response.get(NOTICE_HEADER):
                        # ONE raises a notice starting '!!!' as a warning, so it must go first
                        notice = (f'{notice} {existing}' if notice.startswith('!!!')
                                  else f'{existing} {notice}')
                    response[NOTICE_HEADER] = notice
                record(request, user)
        except Exception:
            # Usage accounting must never cost anyone their response.
            logger.exception('Client usage middleware failed')
        return response

# client_usage

A Django app for Alyx that records, per account and day, which client software made
authenticated requests: ONE and its version, other HTTP libraries, or a browser. It can also
attach a notice to every response sent to a given account, tell users of outdated ONE releases
to update, and estimate how many distinct callers share an account such as `intbrainlab`.

It also records the number of accounts that joined each day (`Signups`), public and otherwise,
once the day has ended.

Only daily aggregates are stored. For accounts listed as shared, a keyed per-day hash of each
caller's address and User-Agent is kept until that day is compacted (`client_usage purge`),
then reduced to a count. Raw addresses and User-Agent strings are never stored.

## Installation

```shell
uv pip install "git+https://github.com/int-brain-lab/iblalyx#subdirectory=client_usage"
```

In `settings_lab.py`:

```python
EXTRA_INSTALLED_APPS = ('client_usage',)
EXTRA_MIDDLEWARE = ('client_usage.middleware.ClientUsageMiddleware',)

CLIENT_USAGE_SHARED_ACCOUNTS = ('intbrainlab',)  # count distinct callers of these
CLIENT_USAGE_NOTICES = {  # sent in the Alyx-API-Notice header to these accounts
    'intbrainlab': '!!!The shared public account will be retired on <date>. Register your own '
                   'account at https://openalyx.internationalbrainlab.org/signup',
}
```

then `manage.py migrate client_usage`.

From release 3.6.1, ONE logs a notice, or raises it as a `RuntimeWarning` if it starts with `!!!`.
A notice starting with `!!!` is put before any other already in the header, such as Alyx's own
advice on paging, since ONE only looks at the start.

### Telling users of outdated ONE releases to update

ONE releases before 3.6.1 never show a notice. Instead, a fraction of their requests can be
refused with status 426 and the notice as the error, which every ONE release raises as an
`HTTPError` showing the message:

```python
CLIENT_USAGE_OUTDATED_RATE = 0.1  # refuse this fraction of outdated clients' requests
CLIENT_USAGE_OUTDATED_NOTICES = {  # the error for these accounts, instead of the default
    'intbrainlab': 'The shared public account intbrainlab will be retired on 1 January 2027. '
                   'Register your own account at https://openalyx.internationalbrainlab.org/signup '
                   'and update ONE with `pip install -U ONE-api`, which your own account requires.',
}
```

A client is outdated if it sends an `ONE-API-Version` older than `CLIENT_USAGE_MIN_ONE_VERSION`,
or sends no version and the requests library's User-Agent: ONE before 3.6.0, though other Python
scripts using requests look the same. Only GET and HEAD requests by an authenticated account are
refused, and never those for the cache tables (`/cache/info`, `/cache.zip`): ONE swallows an
error loading them and sends every search to the database instead. The decision is made before
the view runs, so a refused request does no work.

Each caller, meaning an account at one address with one User-Agent, is refused at most once per
`CLIENT_USAGE_OUTDATED_COOLDOWN` seconds; otherwise a script making many requests would likely
meet several refusals. The cooldowns are kept in Django's default cache. Alyx configures none, so
that is the local-memory cache of each server process, which forgets them when the process
restarts (mod_wsgi's `maximum-requests`) and does not share them between processes. A shared
cache, such as Django's `DatabaseCache`, makes the cooldown exact.

### Keeping the records in a separate database

On a database that is replaced wholesale, such as openalyx at each data release, keep the
records in a database of their own so that their history survives:

```python
EXTRA_DATABASES = {'usage': {
    'ENGINE': 'django.db.backends.postgresql',
    'NAME': 'openalyx_usage',
    'USER': os.getenv('POSTGRES_USER'),
    'PASSWORD': os.getenv('POSTGRES_PASSWORD'),
    'HOST': os.getenv('POSTGRES_HOST'),
    'PORT': os.getenv('POSTGRES_PORT', '5432'),
}}
DATABASE_ROUTERS = ['client_usage.router.ClientUsageRouter']
CLIENT_USAGE_DATABASE = 'usage'
```

then `manage.py migrate client_usage --database usage`.

## Settings

| Setting | Default | |
| --- | --- | --- |
| `CLIENT_USAGE_DATABASE` | `'default'` | Database alias holding the records. |
| `CLIENT_USAGE_FLUSH_SECONDS` | `60` | How often each process writes its buffered counts. |
| `CLIENT_USAGE_SHARED_ACCOUNTS` | `()` | Usernames whose distinct callers are counted. |
| `CLIENT_USAGE_NOTICES` | `{}` | `{username: message}` sent in the `Alyx-API-Notice` header. |
| `CLIENT_USAGE_OUTDATED_RATE` | `0` | Fraction of outdated clients' requests refused; 0 refuses none. |
| `CLIENT_USAGE_OUTDATED_COOLDOWN` | `3600` | Seconds before a refused caller may be refused again; 0 for none. |
| `CLIENT_USAGE_MIN_ONE_VERSION` | `'3.6.1'` | ONE releases before this are outdated. |
| `CLIENT_USAGE_OUTDATED_MESSAGE` | asks for an update | The error refused requests carry. |
| `CLIENT_USAGE_OUTDATED_NOTICES` | `{}` | `{username: message}` carried instead, for these accounts. |
| `CLIENT_USAGE_RETENTION_DAYS` | `400` | Default `--keep-days` for `client_usage purge`. |

## Commands

```shell
manage.py client_usage report [--days 30] [--account intbrainlab]
manage.py client_usage purge [--keep-days 400]  # run daily, e.g. from cron; also records signups
```

## Tests

From an Alyx checkout with this app installed and in `EXTRA_INSTALLED_APPS`:

```shell
python manage.py test client_usage
```

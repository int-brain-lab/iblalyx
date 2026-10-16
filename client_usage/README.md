# client_usage

A Django app for Alyx that records, per account and day, which client software made
authenticated requests: ONE and its version, other HTTP libraries, or a browser. It can also
attach a notice to every response sent to a given account, and estimate how many distinct
callers share an account such as `intbrainlab`.

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
    'intbrainlab': 'The shared public account will be retired on <date>. Register your own '
                   'account at https://openalyx.internationalbrainlab.org/signup',
}
```

then `manage.py migrate client_usage`.

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
| `CLIENT_USAGE_RETENTION_DAYS` | `400` | Default `--keep-days` for `client_usage purge`. |

## Commands

```shell
manage.py client_usage report [--days 30] [--account intbrainlab]
manage.py client_usage purge [--keep-days 400]  # run daily, e.g. from cron
```

## Tests

From an Alyx checkout with this app installed and in `EXTRA_INSTALLED_APPS`:

```shell
python manage.py test client_usage
```

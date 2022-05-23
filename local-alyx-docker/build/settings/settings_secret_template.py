# You should edit this file to match your settings and copy it to
# "settings_secret.py".

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = "%DJANGO_SECRET_KEY%"

S3_ACCESS = {}  # should include the keys (access_key, secret_key, region)

# Database
# https://docs.djangoproject.com/en/1.9/ref/settings/#databases

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql_psycopg2",
        "NAME": "%PGDATABASE%",
        "USER": "%PGUSER%",
        "PASSWORD": "%PGPASSWORD%",
        "HOST": "%PGHOST%",
        "PORT": "5432",
        "OPTIONS": {"options": "-c default_transaction_read_only=%PGREADONLY%"},
    }
}

EMAIL_HOST = "mail.superserver.net"
EMAIL_HOST_USER = "alyx@awesomedomain.org"
EMAIL_HOST_PASSWORD = "UnbreakablePassword"
EMAIL_PORT = 587
EMAIL_USE_TLS = True

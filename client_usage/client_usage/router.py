from django.conf import settings

APP_LABEL = 'client_usage'


def usage_database():
    """The database alias holding the usage tables (setting CLIENT_USAGE_DATABASE)."""
    return getattr(settings, 'CLIENT_USAGE_DATABASE', 'default')


class ClientUsageRouter:
    """Keeps the usage tables in CLIENT_USAGE_DATABASE, and only them there.

    Needed only when that is not 'default', e.g. on a public database whose main database is
    replaced at each data release while the usage history must carry on.
    """

    def db_for_read(self, model, **hints):
        return usage_database() if model._meta.app_label == APP_LABEL else None

    db_for_write = db_for_read

    def allow_migrate(self, db, app_label, **hints):
        usage_db = usage_database()
        if app_label == APP_LABEL:
            return db == usage_db
        if db == usage_db and usage_db != 'default':
            return False  # nothing else belongs in a dedicated usage database
        return None

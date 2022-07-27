"""
For each release tag in OpenAlyx, generate a separate set of parquet tables on S3 containing
only those sessions/datasets
"""
import urllib.parse

from alyx.settings import TABLES_ROOT
from data.models import Tag
import misc.management.commands.one_cache as cache

assert TABLES_ROOT[-1] != '/'

for tag in Tag.objects.all().values_list('name', flat=True):
    print(f'Generating cache for release "{tag}"')
    cmd = cache.Command()
    cmd.handle(
        tag=tag, compress=True, verbosity=1, tables=('sessions', 'datasets'), int_id=False,
        destination=TABLES_ROOT + '/' + urllib.parse.quote(tag)
    )

"""
This module provides a ONE class that uses the SDSC filesystem as a cache.
The purpose is to provide an API that is compatible with the standard ONE API for running on the Popeye cluster.

The specificity of this implementation arises from several factors:
-   the cache is read-only
-   the cache is a constant
-   each file is stored with an UUID between the file stem and the extension

The limitations of this implementation are:
-   alfio.load methods will load objects with long keys containing UUIDS

Recommended usage: just monkey patch the ONE import and run your code as usual on Popeye !
>>> from deploy.iblsdsc import OneDjango as ONE

"""
from typing import Union, Iterable as Iter
from itertools import filterfalse
from datetime import date, datetime, timedelta
from uuid import UUID
import logging
import packaging
import warnings
from pathlib import Path
import io
import urllib.parse
from inspect import unwrap
from functools import partial

import numpy as np
import pandas as pd
from django.urls import resolve
from django.urls.exceptions import Resolver404
from django.http import HttpRequest, QueryDict
from requests.models import Request, Response
from requests.utils import default_user_agent

from iblutil.util import flatten, ensure_list
from one import util
from one.alf.spec import QC, is_uuid, is_uuid_string, is_session_path
from one.api import OneAlyx, One
import one.alf.path as alfiles
import one.params
from one.converters import ConversionMixin
from one.webclient import AlyxClient, _cache_response

from actions.models import Session
from actions.serializers import SessionDetailSerializer, SessionListSerializer
from actions.views import SessionFilter
from misc import views as mv
from misc.models import LabMember

_logger = logging.getLogger(__name__)
CACHE_DIR = Path('/mnt/sdceph/users/ibl/data')
CACHE_DIR_FI = Path('/mnt/ibl')


class AlyxDjango(AlyxClient):

    @_cache_response
    def _generic_request(self, reqfunction, rest_query, data=None, files=None):
        fcn = unwrap(super()._generic_request)
        return fcn(self, partial(self._dispath, reqfunction), rest_query, data, files)

    @staticmethod
    def _prepare_request(method, url, headers, data, files):
        # Building a client-side Request object is not really necessary,
        # however this does change the content type based on the input args.
        # Modified content type probably doesn't matter as we set the POST and FILES
        # directly instead of decoding
        req = Request(
            method=method.__name__.upper(),
            url=url,
            headers=headers,
            files=files,
            data=data or {},
            json=None,
            params={}
        )
        request = req.prepare()

        # Build HttpRequest by setting correct fields
        parsed = urllib.parse.urlparse(url)
        if parsed.params:
            raise NotImplementedError
        headers = {k.upper().replace('-', '_'): v for k, v in (request.headers or {}).items()}
        if 'ACCEPT' in headers:  # must start with HTTP
            headers['HTTP_ACCEPT'] = headers.pop('ACCEPT')
        req = HttpRequest()
        req.method = request.method
        req.path = parsed.path
        req.GET = QueryDict(query_string=parsed.query)
        req._set_content_type_params(headers)  # Set content_type, content_params, and encoding
        req._dont_enforce_csrf_checks = True  # We're not using security cookies
        body = request.body if isinstance(request.body, bytes) else (request.body or '').encode()
        req._stream = io.BytesIO(body)
        req._read_started = False
        if files:
            req.FILES = req._files = files
        req.POST = req._post = data or QueryDict()
        req.META = {
            'SERVER_NAME': parsed.netloc.split(':')[0], 'SERVER_PORT': parsed.port,
            'SERVER_PROTOCOL': parsed.scheme.upper(), 'REQUEST_METHOD': 'GET',
            'QUERY_STRING': parsed.query, 'REQUEST_URI': url[url.index(parsed.netloc) + len(parsed.netloc):],
            'HTTP_USER_AGENT': 'AlyxDjango/1.0.0 ' + default_user_agent(), **headers}

        # Authenticate with token
        req.user = LabMember.objects.get(auth_token=req.META['AUTHORIZATION'].split()[-1])
        return req

    @staticmethod
    def _dispath(method, url, stream=True, headers=None, data=None, files=None):
        request = AlyxDjango._prepare_request(method, url, headers, data, files)

        # Resolve path to get view class
        """
        /subjects ok
        /subjects/ fail
        /subjects/?nickname=SP060 fail (detail)
        /subjects?nickname=SP060 (detail)
        /docs fail
        /docs/ ok
        """
        try:
            match = resolve(request.path)
        except Resolver404:
            match = resolve(request.path + '/')
        request.resolver_match = match

        # Instantiate View instance
        view = match.func.view_class.as_view(**match.func.view_initkwargs)

        # Dispatch request
        t0 = datetime.now()
        response = view(request, *match.args, **match.kwargs)
        rendered = response.render()

        # Convert Django HttpResponse to requests Response object
        r = Response()
        r.status_code = rendered.status_code
        r.reason = rendered.reason_phrase
        # Total elapsed time of the request (approximately)
        r.elapsed = datetime.now() - t0
        r._content = rendered.content
        r.cookies = rendered.cookies
        r.url = url
        r.headers = rendered.headers
        # r.links = rendered.data.links
        r.streaming = rendered.streaming
        return r


class OneDjango(OneAlyx):

    def __init__(self, *, cache_dir=CACHE_DIR_FI, mode='auto', wildcards=True,
                 tables_dir=None, uuid_filenames=False, **kwargs):
        """ONE with direct Django queries."""
        if not tables_dir:
            # Ensure parquet tables downloaded to separate location to the dataset repo
            tables_dir = one.params.get_cache_dir()  # by default this is user downloads
        # Load Alyx Web client
        self._web_client = AlyxDjango(cache_dir=cache_dir, **kwargs)
        self._search_endpoint = 'sessions'
        # get parameters override if inputs provided
        super(OneAlyx, self).__init__(
            cache_dir=cache_dir, mode=mode, wildcards=wildcards, tables_dir=tables_dir)
        # assign property here as it is set by the parent OneAlyx class at init
        self.uuid_filenames = uuid_filenames

    def load_cache(self, tables_dir=None, clobber=False, tag=None):
        """
        Load parquet cache files.  If the local cache is sufficiently old, this method will query
        the database for the location and creation date of the remote cache.  If newer, it will be
        download and loaded.

        Note: Unlike refresh_cache, this will always reload the local files at least once.

        Parameters
        ----------
        tables_dir : str, pathlib.Path
            An optional directory location of the parquet files, defaults to One._tables_dir.
        clobber : bool
            If True, query Alyx for a newer cache even if current (local) cache is recent.
        tag : str
            An optional Alyx dataset tag for loading cache tables containing a subset of datasets.

        Examples
        --------
        To load the cache tables for a given release tag
        >>> one.load_cache(tag='2022_Q2_IBL_et_al_RepeatedSite')

        To reset the cache tables after loading a tag
        >>> ONE.cache_clear()
        ... one = ONE()
        """
        cache_meta = self._cache.get('_meta', {})
        raw_meta = cache_meta.get('raw', {}).values() or [{}]
        # If user provides tag that doesn't match current cache's tag, always download.
        # NB: In the future 'database_tags' may become a list.
        current_tags = flatten(x.get('database_tags') for x in raw_meta)
        if len(set(filter(None, current_tags))) > 1:
            raise NotImplementedError(
                'Loading cache tables with multiple tags is not currently supported'
            )
        tag = tag or current_tags[0]  # For refreshes take the current tag as default
        different_tag = any(x != tag for x in current_tags)
        if not (clobber or different_tag):
            super(OneAlyx, self).load_cache(tables_dir)  # Load any present cache
            expired = self._cache and (cache_meta := self._cache.get('_meta', {}))['expired']
            if not expired or self.mode in {'local', 'remote'}:
                return

        # Warn user if expired
        if (
            cache_meta['expired'] and
            cache_meta.get('created_time', False) and
            not self.alyx.silent
        ):
            age = datetime.now() - cache_meta['created_time']
            t_str = (f'{age.days} day(s)'
                     if age.days >= 1
                     else f'{np.floor(age.seconds / (60 * 2))} hour(s)')
            _logger.info(f'cache over {t_str} old')

        try:
            # Determine whether a newer cache is available
            try:
                cache_info = mv._get_cache_info(tag)
            except FileNotFoundError as ex:
                raise ex from FileNotFoundError('Failed to query cache info. Likely TABLES_DIR'
                                                'field in Alyx settings not correctly set.')
            assert tag is None or tag in cache_info.get('database_tags', [])

            # Check version compatibility
            min_version = packaging.version.parse(cache_info.get('min_api_version', '0.0.0'))
            if packaging.version.parse(one.__version__) < min_version:
                warnings.warn(f'Newer cache tables require ONE version {min_version} or greater')
                return

            # Check whether remote cache more recent
            remote_created = datetime.fromisoformat(cache_info['date_created'])
            local_created = cache_meta.get('created_time', None)
            fresh = local_created and (remote_created - local_created) < timedelta(minutes=1)
            if fresh and not different_tag:
                _logger.info('No newer cache available')
                return

            # Set the cache table directory location
            if tables_dir:  # If tables directory specified, use that
                self._tables_dir = Path(tables_dir)
            elif different_tag:  # Otherwise use a subdirectory for a given tag
                self._tables_dir = self.cache_dir / tag
                self._tables_dir.mkdir(exist_ok=True)
            else:  # Otherwise use the previous location (default is the data cache directory)
                self._tables_dir = self._tables_dir or self.cache_dir

            # Check if the origin has changed. This is to warn users if downloading from a
            # different database to the one currently loaded.
            prev_origin = list(set(filter(None, (x.get('origin') for x in raw_meta))))
            origin = cache_info.get('origin', 'unknown')
            if prev_origin and origin not in prev_origin:
                warnings.warn(
                    'Downloading cache tables from another origin '
                    f'("{origin}" instead of "{", ".join(prev_origin)}")')

            # Download the remote cache files
            _logger.info('Downloading remote caches...')
            files = self.alyx.download_cache_tables(cache_info.get('location'), self._tables_dir)
            assert any(files)
            super(OneAlyx, self).load_cache(self._tables_dir)  # Reload cache after download
        except FileNotFoundError as ex:
            # NB: this error is only raised in online mode
            raise ex from FileNotFoundError(
                f'Cache directory not accessible: {tables_dir or self.cache_dir}\n'
                'Please provide valid tables_dir / cache_dir kwargs '
                'or run ONE.setup to update the default directory.'
            )
        except Exception as ex:
            _logger.debug(ex)
            _logger.error(f'{type(ex).__name__}: Failed to load the remote cache file')
            self.mode = 'remote'

    def load_object(self, *args, **kwargs):
        # call superclass method
        obj = super().load_object(*args, **kwargs)
        if isinstance(obj, list) or not self.uuid_filenames:
            return obj
        # pops the UUID in the key names
        for k in obj.keys():
            new_key = '.'.join(filterfalse(is_uuid_string, k.split('.')))
            obj[new_key] = obj.pop(k)
        return obj

    def _download_datasets(self, dset, **kwargs):
        """Simply return list of None."""
        urls = self._dset2url(dset, update_cache=False)  # normalizes input to list
        return [None] * len(urls)

    def list_datasets(
            self, eid=None, filename=None, collection=None, revision=None, qc=QC.FAIL,
            ignore_qc_not_set=False, details=False, query_type=None, **kwargs
    ) -> Union[np.ndarray, pd.DataFrame]:
        filters = dict(
            collection=collection, filename=filename, revision=revision,
            qc=qc, ignore_qc_not_set=ignore_qc_not_set)
        if (query_type or self.mode) != 'remote':
            return One.list_datasets(self, eid, details=details, query_type=query_type, **filters)
        if not eid:
            raise NotImplementedError
        eid = self.to_eid(eid)  # endure UUID
        if not eid:
            return self._cache['datasets'].iloc[0:0] if details else []  # Return empty
        session = Session.objects.get(pk=eid)
        data = SessionDetailSerializer(session, context={'request': None}).data
        session, datasets = util.ses2records(data)
        # Add to cache tables
        self._update_cache_from_records(sessions=session, datasets=datasets.copy())
        if datasets is None or datasets.empty:
            return self._cache['datasets'].iloc[0:0] if details else []  # Return empty
        assert set(datasets.index.unique('eid')) == {str(eid)}
        datasets = util.filter_datasets(
            datasets.droplevel('eid'), assert_unique=False, wildcards=self.wildcards, **filters)
        # Return only the relative path
        return datasets if details else datasets['rel_path'].sort_values().values.tolist()

    def search(self, details=False, query_type=None, **kwargs):
        query_type = query_type or self.mode
        if query_type != 'remote':
            return One.search(self, details=details, query_type=query_type, **kwargs)
        search_terms = list(SessionFilter.get_filters().keys())
        params = {'django': kwargs.pop('django', '')}
        for key, value in sorted(kwargs.items()):
            field = util.autocomplete(key, search_terms)  # Validate and get full name
            # check that the input matches one of the defined filters
            if field == 'date_range':
                params[field] = ','.join(x.date().isoformat() for x in util.validate_date_range(value))
            elif field == 'dataset':
                if not isinstance(value, str):
                    raise TypeError(
                        '"dataset" parameter must be a string. For lists use "datasets"')
                query = f'data_dataset_session_related__name__icontains,{value}'
                params['django'] += (',' if params['django'] else '') + query
            elif field == 'laboratory':
                params['lab'] = value
            else:
                params[field] = value

        session_filter = SessionFilter(data=params)
        qs = session_filter.qs
        ses = [SessionListSerializer(s, context={'request': None}).data
               for s in SessionListSerializer.setup_eager_loading(qs)]
        # Update cache table with results
        if len(ses) != 0:
            self._update_sessions_table(ses)

        eids = [x['id'] for x in ses]
        if not details:
            return eids

        for s in ses:
            s['date'] = datetime.fromisoformat(s['start_time']).date()

        return eids, ses

    def search_insertions(self, details=False, query_type=None, **kwargs):
        raise NotImplementedError

    def ref2eid(self, ref):
        """
        Returns experiment uuid, given one or more experiment references.

        Parameters
        ----------
        ref : str, dict, list
            One or more objects with keys ('subject', 'date', 'sequence'), or strings with
            the form yyyy-mm-dd_n_subject.

        Returns
        -------
        str, list
            One or more experiment uuid strings.

        Examples
        --------
        >>> base = 'https://test.alyx.internationalbrainlab.org'
        >>> one = ONE(username='test_user', password='TapetesBloc18', base_url=base)
        Connected to...
        >>> ref = {'date': datetime(2018, 7, 13).date(), 'sequence': 1, 'subject': 'flowers'}
        >>> one.ref2eid(ref)
        '4e0b3320-47b7-416e-b842-c34dc9004cf8'
        >>> one.ref2eid(['2018-07-13_1_flowers', '2019-04-11_1_KS005'])
        ['4e0b3320-47b7-416e-b842-c34dc9004cf8',
         '7dc3c44b-225f-4083-be3d-07b8562885f4']
        """
        if self.mode == 'local':
            raise NotImplementedError
        if not isinstance(ref, (dict, str)):
            return list(map(OneDjango.ref2eid, ref))
        ref = ConversionMixin.ref2dict(ref, parse=False)  # Ensure dict
        session = Session.objects.select_related('subject').get(
            subject__nickname=ref['subject'], start_time__date=ref['date'], number=int(ref['sequence']))
        return session

    def to_eid(self, session_id):
        if session_id is None:
            return
        elif is_uuid(session_id):
            return session_id
        elif self.is_exp_ref(session_id):
            return self.ref2eid(session_id)
        elif isinstance(session_id, dict):
            assert {'subject', 'number', 'lab'}.issubset(session_id)
            session_id = self.cache_dir.joinpath(
                session_id['lab'],
                'Subjects', session_id['subject'],
                str(session_id.get('date') or session_id['start_time'][:10]),
                ('%03d' % session_id['number']))
        if isinstance(session_id, Path):
            return self.path2eid(session_id)
        elif isinstance(session_id, str):
            if is_session_path(session_id) or alfiles.get_session_path(session_id):
                return self.path2eid(session_id)
            if len(session_id) > 36:
                session_id = session_id[-36:]
            if not is_uuid_string(session_id):
                raise ValueError('Invalid experiment ID')
            else:
                return UUID(session_id)
        elif isinstance(session_id, Iter):
            return list(map(self.to_eid, session_id))
        else:
            raise ValueError('Unrecognized experiment ID')

    @staticmethod
    def path2django(path_obj):
        """
        From a local path, gets the Session record.

        Parameters
        ----------
        path_obj : pathlib.Path, str, list
            Local path or list of local paths.

        Returns
        -------
        Session, list of Session
            The Session records.

        Raises
        ------
        Session.DoesNotExist
            Session does not exist on Alyx or is invalid.
        """
        sessions = Session.objects.select_related('lab', 'subject')
        return_list = not isinstance(path_obj, (str, Path))
        ret = []
        for session_path in map(alfiles.get_session_path, ensure_list(path_obj)):
            if not session_path:
                raise Session.DoesNotExist(f'Invalid session: {path_obj}')
            lab, subject, session_date, number = alfiles.session_path_parts(session_path)
            args = {'subject__nickname': subject, 'start_time__date': date.fromisoformat(session_date), 'number': int(number)}
            if lab:
                args['lab'] = lab
            session = sessions.get(**args)
            ret.append(session)
        return ret if return_list else ret[0]

    def path2eid(self, path_obj, query_type=None):
        """
        From a local path, gets the experiment id.

        Parameters
        ----------
        path_obj : pathlib.Path, str, list
            Local path or list of local paths.
        query_type : str
            If set to 'remote', will force database connection.

        Returns
        -------
        UUID, list
            Experiment UUID (eid) or list of eids.

        Raises
        ------
        Session.DoesNotExist
            Session does not exist on Alyx or is invalid.
        """
        if (query_type or self.mode) != 'remote':
            cache_eid = One.path2eid(self, path_obj)
            if cache_eid or self.mode == 'local':
                return cache_eid

        r = OneDjango.path2django(path_obj)
        return [s.pk for s in r] if isinstance(r, list) else r.pk

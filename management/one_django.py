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
from datetime import date
from uuid import UUID
import logging
from pathlib import Path

import numpy as np
import pandas as pd

from one import util
from one.alf.spec import QC, is_uuid, is_uuid_string, is_session_path
from one.api import One
import one.alf.files as alfiles
import one.params
from one.converters import ConversionMixin

from actions.models import Session
from actions.serializers import SessionDetailSerializer

_logger = logging.getLogger(__name__)
CACHE_DIR = Path('/mnt/sdceph/users/ibl/data')
CACHE_DIR_FI = Path('/mnt/ibl')


class OneDjango(One):

    def __init__(self, *args, cache_dir=CACHE_DIR_FI, uuid_filenames=False, **kwargs):
        if not kwargs.get('tables_dir'):
            # Ensure parquet tables downloaded to separate location to the dataset repo
            kwargs['tables_dir'] = one.params.get_cache_dir()  # by default this is user downloads
        super().__init__(*args, cache_dir=cache_dir, **kwargs)
        # assign property here as it is set by the parent OneAlyx class at init
        self.uuid_filenames = uuid_filenames

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
        urls = self._dset2url(self, dset, update_cache=False)  # normalizes input to list
        return [None] * len(urls)

    def list_datasets(
            self, eid=None, filename=None, collection=None, revision=None, qc=QC.FAIL,
            ignore_qc_not_set=False, details=False, **kwargs
    ) -> Union[np.ndarray, pd.DataFrame]:
        if not eid:
            raise NotImplementedError
        eid = self.to_eid(eid)  # endure UUID
        if not eid:
            return self._cache['datasets'].iloc[0:0] if details else []  # Return empty
        session = Session.objects.get(pk=eid)
        data = SessionDetailSerializer(session, context={'request': None}).data
        filters = dict(
            collection=collection, filename=filename, revision=revision,
            qc=qc, ignore_qc_not_set=ignore_qc_not_set)
        session, datasets = util.ses2records(data)
        # Add to cache tables
        self._update_cache_from_records(sessions=session, datasets=datasets.copy())
        if datasets is None or datasets.empty:
            return self._cache['datasets'].iloc[0:0] if details else []  # Return empty
        assert set(datasets.index.unique('eid')) == {eid}
        datasets = util.filter_datasets(
            datasets.droplevel('eid'), assert_unique=False, wildcards=self.wildcards, **filters)
        # Return only the relative path
        return datasets if details else datasets['rel_path'].sort_values().values.tolist()

    @staticmethod
    def ref2eid(ref):
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
        for session_path in map(alfiles.get_session_path, util.ensure_list(path_obj)):
            if not session_path:
                raise Session.DoesNotExist(f'Invalid session: {path_obj}')
            lab, subject, session_date, number = alfiles.session_path_parts(session_path)
            session = sessions.get(
                lab__name=lab, subject__nickname=subject, start_time__date=date.fromisoformat(session_date), number=int(number))
            ret.append(session)
        return ret if return_list else ret[0]

    @staticmethod
    def path2eid(path_obj):
        """
        From a local path, gets the experiment id.

        Parameters
        ----------
        path_obj : pathlib.Path, str, list
            Local path or list of local paths.

        Returns
        -------
        UUID, list
            Experiment UUID (eid) or list of eids.

        Raises
        ------
        Session.DoesNotExist
            Session does not exist on Alyx or is invalid.
        """
        r = OneDjango.path2django(path_obj)
        return [s.pk for s in r] if isinstance(r, list) else r.pk

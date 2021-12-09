"""This script will comb the public repository and remove any datasets that are not tagged as
public, or do not exist on Open Alyx"""
# this code is meant to run on the SDSC flatiron instance
from pathlib import Path
import logging
from itertools import chain

import numpy as np
from one.api import ONE
from one.alf.files import get_alf_path, add_uuid_string, session_path_parts
from one.alf.io import iter_sessions
from one.alf import spec
from one.alf.cache import _iter_datasets
from iblutil.io.parquet import np2str

import ibllib  # For logger

log = logging.getLogger('ibllib')
SDSC_PUBLIC_PATH = Path('/mnt/ibl/public')
DRY_RUN = True
"""Strictness levels:

    0 - adds missing datasets that are on Open Alyx
    1 - additionally removes ALF datasets that don't exist on Open Alyx
    2 - additionally removes Alyx datasets that aren't tagged as public
    3 - additionally removes all files that don't exist on Open Alyx
"""
STRICTNESS = 0

one = ONE(base_url='https://openalyx.internationalbrainlab.org',
          username='intbrainlab', password='international')
if STRICTNESS == 0:
    log.info('Linking missing Open Alyx datasets')
    n = 0
    for eid in one.search():
        rel_path = get_alf_path(one.eid2path(eid))
        session_path = SDSC_PUBLIC_PATH.joinpath('..', rel_path).resolve()
        dsets = one.list_datasets(eid, details=True)
        dids = np2str(np.array(dsets.index.values.tolist()))
        for dset, did in zip(dsets['rel_path'].values, dids):
            dset = add_uuid_string(dset, did)
            dpath = session_path / dset
            link_path = SDSC_PUBLIC_PATH.joinpath(rel_path, dset)
            if link_path.exists():
                log.debug('f"skipped existing {link_path}"')
                continue
            if not dpath.exists():
                log.error(f'source file doesn\'t exist: {dpath}')
                log.error(f'eid = {eid}; did = {did}')
                continue
            if not DRY_RUN:
                link_path.parent.mkdir(exist_ok=True, parents=True)
                link_path.symlink_to(dpath)
            log.info(f'linked {link_path}')
            n += 1
    log.info(f'{n} new datasets linked')

if STRICTNESS >= 1:
    log.info('Removing missing ALF datasets')
    n = 0
    for session_path in iter_sessions(SDSC_PUBLIC_PATH):
        for dataset_path in _iter_datasets(session_path):
            name_parts = Path(dataset_path).name.split('.')
            if not spec.is_uuid_string(name_parts[-2]):
                det = session_path_parts(session_path, as_dict=True)
                det = one.alyx.rest('datasets', 'list',
                                    date=det['date'],
                                    experiment_number=det['number'],
                                    subject=det['subject'],
                                    lab=det['lab'],
                                    name=Path(dataset_path).name,
                                    collection=Path(dataset_path).parent.as_posix())
            else:
                uuid = name_parts[-2]
                det = one.alyx.rest('datasets', 'list', id=uuid)
            if not len(det):
                log.warning(f'{session_path / dataset_path} not found on database')
                if not DRY_RUN:
                    session_path.joinpath(dataset_path).unlink()
                log.info(f'unlinked {session_path / dataset_path}')
                n += 1
            else:
                if STRICTNESS >= 2 and not det['public']:
                    # FIXME At the moment tags not marked as public on Open Alyx; should check main
                    #  Alyx instead
                    raise NotImplementedError()
                    log.warning(f'{session_path / dataset_path} not tagged as public')
                    if not DRY_RUN:
                        session_path.joinpath(dataset_path).unlink()
                    log.info(f'unlinked {session_path / dataset_path}')
                    n += 1

    log.info(f'{n} ALF datasets unlinked')

if STRICTNESS >= 3:
    log.info('Removing all files missing from Open Alyx')
    raise NotImplementedError()

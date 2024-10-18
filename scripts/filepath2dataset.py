"""This function demonstrates how to convert one or more dataset file paths to an Alyx dataset record."""
from datetime import date
from collections import defaultdict

import one.alf.files as alfiles

from subjects.models import Subject
from actions.models import Session
from data.models import Dataset


def files2datasets(file_list):
    """Return a list of dataset records from file_list.

    This method is only useful for file paths that don't have a dataset UUID in the filename.

    Parameters
    ----------
    file_list : list of pathlib.Path
        A list of dataset file paths.

    Returns
    -------
    list of Dataset
        A list of corresponding Dataset records.
    """
    file2dset = dict.fromkeys(file_list)
    files_by_session = defaultdict(list)
    # Filter valid files and sort by session
    for f in file_list:
        session_path = alfiles.get_session_path(f)
        files_by_session[session_path].append(f.relative_to(session_path).as_posix())

    for session_path, datasets in files_by_session.items():
        lab, subject, session_date, number = alfiles.session_path_parts(session_path)
        subject = Subject.objects.get(nickname=subject)
        sessions = Session.objects.select_related('lab').filter(lab__name=lab, subject=subject,
            start_time__date=date.fromisoformat(session_date), number=int(number))
        if sessions.count():
            session_datasets = (
                Dataset.objects.select_related('revision').prefetch_related('file_records').filter(session__in=sessions))
            for d in datasets:
                collection, revision, *_ = alfiles.rel_path_parts(d, as_dict=False)
                kwargs = {'collection': collection, 'name': alfiles.remove_uuid_string(d).name}
                if revision is None:
                    kwargs['revision__isnull'] = True
                else:
                    kwargs['revision__name'] = revision
                file2dset[session_path / d] = session_datasets.get(**kwargs)
    return file2dset

"""Module for digesting and performing analytics on S3 public bucket access logs"""
from datetime import datetime, timedelta
from calendar import monthrange
from pathlib import PurePosixPath, Path
import warnings
from time import sleep
import pickle
from urllib.request import urlopen
from ipaddress import IPv4Address, IPv6Address
import json
import time

import pandas as pd
import numpy as np
import pyarrow as pa
import boto3
from botocore.exceptions import ClientError
from one.alf.path import get_session_path
from itertools import zip_longest

from . import io as s3io


def files_accessed_this_month():
    """Print the number of times each file was accessed this month"""
    # https://aws.amazon.com/blogs/storage/monitor-amazon-s3-activity-using-s3-server-access-logs-and-pandas-in-python/
    df = s3io.get_log_table_by_month(datetime.today())
    file_access = (df['Operation'] == 'REST.GET.OBJECT') & (df['Key'].str.startswith('data/'))
    print(df.loc[file_access, 'Key'].value_counts())
    return df


def week_of_month(dt):
    """
    Returns the week of the month for the specified date.

    Weeks are numbered from 1-5.  Also returns the date times of the first and last week days.

    Parameters
    ----------
    dt : datetime.datetime
        The datetime for which to determine the week number.

    Returns
    -------
    int
        The week number within the month of dt.
    datetime.datetime
        The datetime of the first day of the week within the month.
    datetime.datetime
        The datetime of the last day of the week within the month.
    """
    _, n_days = monthrange(dt.year, dt.month)
    first_day = dt.replace(day=1)
    adjusted_dom = dt.day + first_day.weekday()
    week_number = int(np.ceil(adjusted_dom / 7))

    first_day = dt.day - dt.weekday()
    if first_day < 1:
        first_day = 1
    last_day = dt.day + (6 - dt.weekday())
    if last_day > n_days:
        last_day = n_days

    start_date = dt.replace(day=first_day).date()
    start = datetime(*start_date.timetuple()[:3])
    end = (start.replace(day=last_day) +
           timedelta(hours=23, minutes=59, seconds=59, milliseconds=999))

    return week_number, (start, end)


def consolidate_logs(boto_session=None, date='last_month', ipinfo_token=None, profile_name='miles'):
    """
    Download last month's log files, upload as parquet table to S3 and delete individual log files.

    Logs are uploaded to the REMOTE_LOG_LOCATION with the following name pattern:
        consolidated/YYYY-MM_<BUCKET-NAME>.pqt
    If the logs are consolidated for the current month, the file will end with '_INCOMPLETE.pqt'.
    The IP address locations are stored in a file with the same name, ending with '_IP-info.pqt'.

    Individual log files are only deleted once the consolidated logs are

    Parameters
    ----------
    boto_session : boto3.Session
        An S3 Session with PUT and DELETE permissions.
    date : str, datetime.datetime, datetime.date
        If 'last_month', consolidate all of last month's logs; if 'this_month', consolidate the
        logs for the current month so far (still deletes the logs); if a date is provided, the logs
        for that date's month are consolidated.
    ipinfo_token : str
        An API token for using with the ipinfo API to query IP address location.
    profile_name: str
        The profile name of the boto s3 credentials

    Returns
    -------
    pandas.DataFrame
        The downloaded logs.
    str
        The URI of the uploaded parquet table.
    """
    today = datetime.utcnow()
    if date == 'this_month':
        partial = True
        start_date = today.replace(day=1).date()
        start = datetime(*start_date.timetuple()[:3])
        end = today
    elif date == 'last_month':
        partial = False
        # The date range for last month
        month = today.month
        start_date = today.replace(
            day=1, month=month - 1 or 12, year=today.year - int(not bool(month - 1))).date()
        start = datetime(*start_date.timetuple()[:3])
        end = (start.replace(day=monthrange(start.year, start.month)[1]) +
               timedelta(hours=23, minutes=59, seconds=59, milliseconds=999))
    else:
        if isinstance(date, str):
            date = datetime.fromisoformat(date)
        partial = (date.year, date.month) == (today.year, today.month)
        start_date = date.replace(day=1).date()
        start = datetime(*start_date.timetuple()[:3])
        end = (start.replace(day=monthrange(start.year, start.month)[1]) +
               timedelta(hours=23, minutes=59, seconds=59, milliseconds=999))

    # Check for parquet file on S3
    session = boto_session or boto3.Session(profile_name=profile_name)
    dst_bucket_name = 'ibl-brain-wide-map-private'
    s3 = session.resource('s3')
    bucket = s3.Bucket(name=dst_bucket_name)
    prefix = f'{s3io.REMOTE_LOG_LOCATION}consolidated/{start.strftime("%Y-%m")}'
    consolidated = (x.key for x in bucket.objects.filter(Prefix=prefix)
                    if not (x.key.endswith('INCOMPLETE.pqt') or x.key.endswith('IP-info.pqt')))
    assert next(iter(consolidated), False) is False, \
        'logs already consolidated for ' + start.strftime('%B')

    print(f'Reading remote logs for {start.strftime("%B")}' + (' so far' if partial else ''))
    try:
        df = s3io.read_remote_logs(date_range=(start, end), log_location=s3io.REMOTE_LOG_LOCATION, s3_bucket=bucket)
    except pd.errors.ParserError as ex:
        # pandas.errors.ParserError: Error tokenizing data. C error: Expected 26 fields in line 2, saw 27
        warnings.warn(f'{ex}\n')
        df = s3io.read_remote_logs_robust(date_range=(start, end), log_location=s3io.REMOTE_LOG_LOCATION,
                                          s3_bucket=bucket)

    # Process table
    df = s3io.prepare_for_parquet(df)
    # Check every row was parsed correctly
    assert all(isinstance(x, str) and len(x) == 64 for x in df.Bucket_Owner.unique())
    if df.empty:
        warnings.warn('No logs found!')
        return df, None

    bucket_name, = df['Bucket'].unique()
    filename = f'{start.strftime("%Y-%m")}_{bucket_name}.pqt'
    s3_url = PurePosixPath(s3io.REMOTE_LOG_LOCATION, 'consolidated', filename)

    # Attempt to download the incomplete logs table and merge
    partial_file = s3_url.with_name(s3_url.stem + '_INCOMPLETE.pqt')
    # Download partial month logs
    filepath = s3io.LOCAL_LOG_LOCATION / (partial_file.stem + f'.tmp{np.floor(time.time()):.0f}.pqt')
    try:
        # Check for partial log table
        s3.Object(bucket_name=dst_bucket_name, key=partial_file.as_posix()).load()
        print('Downloading partial log table')
        bucket.download_file(partial_file.as_posix(), str(filepath))
        df_ = pd.read_parquet(filepath)
        assert np.all(df['Bucket'].unique() == bucket_name), 'multiple bucket logs'
        print(f'Concatenating logs ({df_.size} + {df.size} rows)')
        df = pd.concat([df_, df], ignore_index=True)
    except ClientError as ex:
        if ex.response['Error']['Code'] != '404':
            raise ex

    print('Removing duplicate rows')
    df.drop_duplicates(subset=None, keep='first', inplace=True, ignore_index=True)

    print('Uploading table')
    s3io.upload_table(df, partial_file if partial else s3_url, bucket)

    print('Deleting log files')
    for obj in s3io._iter_objects(s3io.REMOTE_LOG_LOCATION, date_range=(start, end), s3_bucket=bucket):
        assert PurePosixPath(obj.key).name.startswith(start_date.strftime('%Y-%m'))
        print(f'deleting {obj.key}')
        obj.delete()

    if not partial:
        try:
            # Delete incomplete log table if exists
            bucket.objects.filter(Prefix=partial_file.as_posix()).delete()
            print(f'deleted {partial_file}')
        except ClientError as ex:
            if ex.response['Error']['Code'] != '404':
                raise ex

    print('Fetching IP info...')
    # Unique accesses
    unique_ips = df.loc[:, 'Remote_IP'].unique()

    print(f'Data was accessed from {len(unique_ips):,d} unique devices')
    # Attempt to download current IP info table, if exists
    ip_table_url = s3_url.with_name(s3_url.stem + '_IP-info.pqt')
    try:
        # Check for partial log table
        bucket.download_file(ip_table_url.as_posix(), str(s3io.LOCAL_LOG_LOCATION / ip_table_url.name))
        ip_details_ = pd.read_parquet(s3io.LOCAL_LOG_LOCATION / ip_table_url.name)
        unique_ips = np.setdiff1d(unique_ips, ip_details_.index, assume_unique=True)
    except ClientError as ex:
        if ex.response['Error']['Code'] != '404':
            raise ex
        ip_details_ = None

    if unique_ips.any():
        if '-' in unique_ips:
            print(f"Removing unknown IP with address '-'")
            unique_ips = np.setdiff1d(unique_ips, np.array('-'), assume_unique=True)
        print(f'Querying location for {unique_ips.size} IPs')
        ip_details = ip_info(unique_ips, wait=None, token=ipinfo_token)
        ip_details = pd.DataFrame(ip_details).set_index('ip')
        if ip_details_ is not None:
            ip_details = pd.concat([ip_details_, ip_details], verify_integrity=True)
        print('Uploading IP table')
        s3io.upload_table(ip_details, ip_table_url, bucket)

    return df, f's3://{dst_bucket_name}/{partial_file if partial else s3_url}'


def key2date(key: str) -> datetime:
    """
    Convert a access log file key to a datetime object.

    Parameters
    ----------
    key : str
        The location of an S3 log file.

    Returns
    -------
    datetime.datetime
        The datetime parsed from the file name.

    Example
    -------
    >>> key2date('logs/server-access-logs/2023-03-01-00-09-07-9774A35EB7B3DBE3')
    datetime.datetime(2023, 3, 1, 0, 9, 7)
    """
    return datetime(*map(int, PurePosixPath(key).name.split('-')[:-1]))


def _first_log_datetime(s3_bucket=None):
    """Get the datetime of the first S3 log file."""
    obj = next(s3io._iter_objects(s3io.REMOTE_LOG_LOCATION, s3_bucket=s3_bucket))
    return key2date(obj.key)


def consolidate_logs_by_week(boto_session=None):
    """
    Download logs by week, consolidate and upload as parquet table to S3, then delete individual
    the individual log files.

    Parameters
    ----------
    boto_session : boto3.Session
        An S3 Session with PUT and DELETE permissions.

    Returns
    -------
    list of str
        The URIs of the uploaded parquet tables.
    """
    session = boto_session or boto3.Session(profile_name='miles')
    dst_bucket_name = 'ibl-brain-wide-map-private'
    s3 = session.resource('s3')
    bucket = s3.Bucket(name=dst_bucket_name)

    urls = []
    while datetime.utcnow() - (dt := _first_log_datetime()) > timedelta(days=7):
        week_number, (start, end) = week_of_month(dt)

        # Check for parquet file on S3
        consolidated = bucket.objects.filter(
            Prefix=f'{s3io.REMOTE_LOG_LOCATION}/consolidated/{start.strftime("%Y-%m")}_{week_number}')
        assert next(iter(consolidated), False) is False, \
            f'logs already consolidated for week {week_number} of ' + start.strftime('%B')

        print(f'Reading remote logs for week {week_number} of {start.strftime("%B")}')
        try:
            df = s3io.read_remote_logs(date_range=(start, end), log_location=s3io.REMOTE_LOG_LOCATION,
                                       s3_bucket=bucket)
        except pd.errors.ParserError as ex:
            # pandas.errors.ParserError: Error tokenizing data. C error: Expected 26 fields in line 2, saw 27
            warnings.warn(f'{ex}\n')
            df = s3io.read_remote_logs_robust(date_range=(start, end), log_location=s3io.REMOTE_LOG_LOCATION,
                                              s3_bucket=bucket)

        # Process table
        df = s3io.prepare_for_parquet(df)
        # Check every row was parsed correctly
        assert all(isinstance(x, str) and len(x) == 64 for x in df.Bucket_Owner.unique())
        bucket_name, = df['Bucket'].unique()
        filename = f'{start.strftime("%Y-%m")}_{week_number}_{bucket_name}.pqt'
        s3_url = PurePosixPath(s3io.REMOTE_LOG_LOCATION, 'consolidated', filename)

        print('Uploading table')
        s3io.upload_table(df, s3_url, bucket)

        print('Deleting log files')
        for obj in s3io._iter_objects(s3io.REMOTE_LOG_LOCATION, date_range=(start, end), s3_bucket=bucket):
            assert end > key2date(obj.key) > start
            print(f'deleting {obj.key}')
            obj.delete()

        urls.append(f's3://{dst_bucket_name}/{s3_url}')

        print('Fetching IP info...')
        # Unique accesses
        unique_ips = df.loc[:, 'Remote_IP'].unique()

        print(f'Data was accessed from {len(unique_ips):,d} unique devices')
        ip_details = ip_info(unique_ips, wait=.2)  # pause to avoid DoS
        ip_details = pd.DataFrame(ip_details).set_index('ip')
        s3io.upload_table(ip_details, s3_url.with_name(s3_url.stem + '_IP-info.pqt'), bucket)

    return urls


def parse_time_column(df):
    """Converts the Time and Time_Offset string columns to pandas Datetime objects"""
    return pd.to_datetime(df['Time'] + df['Time_Offset'], format='[%d/%b/%Y:%H:%M:%S%z]', utc=True)


def ip_info(ip_address, wait=None, token=None):
    """
    Fetch DNS information associated with the IP address(es).

    Uses the public API of ipinfo.io. Use the wait arg if concerned about reaching request limit
    for large IP lists.

    Parameters
    ----------
    ip_address : iterable, str, IPv4Address, IPv6Address
        One or more IP addresses to look up.
    wait : float, bool, optional
        Whether to wait between API queries to avoid DoS.
    token : str, optional
        An optional API token to use.

    Returns
    -------
    dict, list of dict
        The IP address lookup details.
    """
    if not isinstance(ip_address, (str, IPv4Address, IPv6Address)):
        return [ip_info(ip, wait, token) for ip in ip_address if wait is None or not sleep(wait)]

    url = f'https://ipinfo.io/{ip_address}' + (f'?token={token}' if token else '/json')
    res = urlopen(url)
    assert res
    info = json.load(res)
    info['accessed'] = datetime.utcnow().isoformat()
    return info


def SCGB_renewal():
    """Plots and stats for the SCGB renewal grant"""
    sfn_start_date = pd.Timestamp.fromisoformat('2022-11-12')
    # Download logs for this month
    file_obj = s3io.get_log_table_by_month(sfn_start_date)
    table = pa.BufferReader(file_obj.get()['Body'].read())
    df = pd.read_parquet(table)

    # Print number of files accessed
    file_access = (df['Operation'] == 'REST.GET.OBJECT') & (df['Key'].str.startswith('data/'))
    datasets = df.loc[file_access, 'Key'].unique()
    print(f'{sum(file_access):,} total downloads for the month of {sfn_start_date.strftime("%B")}')
    print(f'{len(datasets):,} different datasets downloaded')

    # Unique accesses
    unique_ips = df.loc[file_access, 'Remote_IP'].unique()
    print(f'Data was accessed from {len(unique_ips)} unique devices')
    _filename = f'{datetime.today().strftime("%Y-%m")}_log_ips.pkl'
    filename = Path.home().joinpath(_filename)
    if filename.exists():
        with open(filename, 'rb') as file:
            ip_info_map = pickle.load(file)
    else:
        ip_info_map = []
        for ip in unique_ips:
            sleep(.2)  # pause to avoid DoS
            ip_info_map.append(ip_info(ip))
        ip_info_map = {x.pop('ip'): x for x in ip_info_map}
        with open(filename, 'wb') as file:
            pickle.dump(ip_info_map, file)

    city = 'San Diego'  # Location of SfN 2022
    sfn_ips = [ip for ip, det in ip_info_map.items() if det['city'] == city]
    at_sfn = df['Remote_IP'].isin(sfn_ips)
    print(f'{len(df.loc[file_access & at_sfn, "Key"]):,} datasets downloaded at SfN alone')

    # N sessions accessed
    sessions = set(filter(None, map(get_session_path, datasets)))
    print(f'{len(sessions):,} unique sessions accessed')

    print(df.loc[file_access, 'Key'].value_counts())
    # ONE cache downloads
    cache_access = (df['Operation'] == 'REST.GET.OBJECT') & \
                   (df['Key'].str.match(r'caches/openalyx/[a-zA-Z0-9_/]*cache.zip'))
    # df.loc[cache_access, 'Key'].unique()
    print(f'Public ONE cache was downloaded a total of {sum(cache_access):,} times, of which '
          f'{sum(cache_access & at_sfn):,} downloads occured at SfN')

    # (for devs) Python versions used
    py_ver = df.loc[file_access, 'User_Agent'].str.extract(r'(?<=Python/)(\d+\.\d+)')
    py_ver[0].hist()



def grouper(iterable, n, *, incomplete='fill', fillvalue=None):
    """Collect data into non-overlapping fixed-length chunks or blocks"""
    # grouper('ABCDEFG', 3, fillvalue='x') --> ABC DEF Gxx
    # grouper('ABCDEFG', 3, incomplete='strict') --> ABC DEF ValueError
    # grouper('ABCDEFG', 3, incomplete='ignore') --> ABC DEF
    args = [iter(iterable)] * n
    if incomplete == 'fill':
        return zip_longest(*args, fillvalue=fillvalue)
    if incomplete == 'strict':
        return zip(*args, strict=True)
    if incomplete == 'ignore':
        return zip(*args)
    else:
        raise ValueError('Expected fill, strict, or ignore')


def get_access(YEAR=None, save_path=None, save=False):
    """
    Get summary of data for all data or given year
    :param YEAR:
    :return:
    """
    columns = [
        'total_file_access', 'unique_file_access', 'total_bytes_sent', 'date', 'unique_ips',
        'unique_countries', 'unique_cities', 'unique_sessions', 'cache_access']
    download_data = pd.DataFrame(columns=columns)

    total_unique = {k: set() for k in ['file_access', 'ips', 'countries', 'cities', 'sessions']}

    for log_obj, ip_obj in grouper(s3io.iter_log_tables(), 2):
        assert 'ibl-brain-wide-map-public' in log_obj.key
        assert ip_obj is None or 'IP-info' in ip_obj.key

        if YEAR and YEAR not in log_obj.key:
            continue  # only look at the year

        if save_path is not None:
            log_obj_path = Path(save_path).joinpath(PurePosixPath(log_obj.key).name)
            if log_obj_path.exists():
                df = pd.read_parquet(log_obj_path)
            else:
                table = pa.BufferReader(log_obj.get()['Body'].read())
                df = pd.read_parquet(table)
                if save:
                    df.to_parquet(log_obj_path)
        else:
            table = pa.BufferReader(log_obj.get()['Body'].read())
            df = pd.read_parquet(table)

        data = {'date': PurePosixPath(log_obj.key).name[:7]}
        # Print number of files accessed
        file_access = (df['Operation'] == 'REST.GET.OBJECT') & (df['Key'].str.startswith('data/'))
        print(f'{sum(file_access):,} total downloads for the month of {data["date"]}')
        print(f'{len(df.loc[file_access, "Key"].unique()):,} different datasets downloaded')
        data['total_file_access'] = sum(file_access)
        data['unique_file_access'] = len(df.loc[file_access, 'Key'].unique())
        data['total_bytes_sent'] = sum(df.loc[file_access, 'Bytes_Sent'])

        # Unique accesses
        unique_ips = df.loc[file_access, 'Remote_IP'].unique()
        total_unique['ips'].update(unique_ips)
        print(f'Data was accessed from {len(unique_ips)} unique devices')
        data['unique_ips'] = len(unique_ips)
        if ip_obj:
            if save_path is not None:
                ip_obj_path = Path(save_path).joinpath(PurePosixPath(ip_obj.key).name)
                if ip_obj_path.exists():
                    ip_info = pd.read_parquet(ip_obj_path)
                else:
                    ip_info = pa.BufferReader(ip_obj.get()['Body'].read())
                    ip_info = pd.read_parquet(ip_info)
                    if save:
                        ip_info.to_parquet(ip_obj_path)
            else:
                ip_info = pa.BufferReader(ip_obj.get()['Body'].read())
                ip_info = pd.read_parquet(ip_info)

            unique_cities = ip_info['city'].str.cat(ip_info[['region', 'country']], sep='/', na_rep='Unknown').unique()
            total_unique['cities'].update(unique_cities)
            data['unique_cities'] = len(unique_cities)
            unique_countries = ip_info['country'].unique()
            data['unique_countries'] = len(unique_countries)
            total_unique['countries'].update(unique_countries)

        # N sessions accessed
        datasets = df.loc[file_access, 'Key'].unique()
        sessions = set(map(get_session_path, datasets))
        print(f'{len(sessions):,} unique sessions accessed')
        data['unique_sessions'] = len(sessions)
        total_unique['sessions'].update(sessions)
        total_unique['file_access'].update(df.loc[file_access, 'Key'].unique())

        # ONE cache downloads
        cache_access = (df['Operation'] == 'REST.GET.OBJECT') & \
                       (df['Key'].str.match(r'caches/openalyx/[a-zA-Z0-9_/]*cache.zip'))
        # df.loc[cache_access, 'Key'].unique()
        print(f'Public ONE cache was downloaded a total of {sum(cache_access):,} times')
        data['cache_access'] = sum(cache_access)

        download_data = pd.concat([download_data, pd.DataFrame(data, index=[0])])

    print(
        f'So far data accessed from {len(total_unique["cities"])} different cities '
        f'across {len(total_unique["countries"])} countries'
    )

    download_data = download_data.set_index('date')

    return download_data, total_unique


def get_file_count_by_ip(save_path=None, save=False):

    all_df = pd.DataFrame()
    for log_obj, ip_obj in grouper(s3io.iter_log_tables(), 2):

        assert 'ibl-brain-wide-map-public' in log_obj.key
        assert ip_obj is None or 'IP-info' in ip_obj.key

        if save_path is not None:
            log_obj_path = Path(save_path).joinpath(PurePosixPath(log_obj.key).name)
            if log_obj_path.exists():
                df = pd.read_parquet(log_obj_path)
            else:
                table = pa.BufferReader(log_obj.get()['Body'].read())
                df = pd.read_parquet(table)
                if save:
                    df.to_parquet(log_obj_path)
        else:
            table = pa.BufferReader(log_obj.get()['Body'].read())
            df = pd.read_parquet(table)

        # Get number of files accessed
        file_access = (df['Operation'] == 'REST.GET.OBJECT')

        info = df.loc[file_access, 'Remote_IP'].value_counts()
        df_ip = pd.DataFrame({'ip': info.index, 'count': info.values,
                              'date': PurePosixPath(log_obj.key).name[:7] })

        if save_path is not None:
            ip_obj_path = Path(save_path).joinpath(PurePosixPath(ip_obj.key).name)
            if ip_obj_path.exists():
                ip_info = pd.read_parquet(ip_obj_path)
            else:
                ip_info = pa.BufferReader(ip_obj.get()['Body'].read())
                ip_info = pd.read_parquet(ip_info)
                if save:
                    ip_info.to_parquet(ip_obj_path)
        else:
            ip_info = pa.BufferReader(ip_obj.get()['Body'].read())
            ip_info = pd.read_parquet(ip_info)

        ip_info = ip_info[['city', 'region', 'country']]
        ip_info = ip_info.reset_index()

        all_df = pd.concat([all_df, df_ip.merge(ip_info, how='outer', on='ip')])

    return all_df
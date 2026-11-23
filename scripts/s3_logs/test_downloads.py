"""Tests for s3_logs.io.summarise_downloads.

Run from the scripts folder with ``python -m unittest s3_logs.test_downloads``.
"""
import unittest
import tempfile
from datetime import datetime
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from unittest import mock

import numpy as np
import pandas as pd

from s3_logs import io as s3io
from s3_logs import process

MB = 1_000_000


def _log(rows):
    """Make a log table from (ip, key, time, bytes, size, ...) tuples with sensible defaults."""
    records = []
    for r in rows:
        ip, key, t, sent, size = r[:5]
        extra = r[5] if len(r) > 5 else {}
        records.append({
            'Time': '[' + pd.Timestamp(t).strftime('%d/%b/%Y:%H:%M:%S'),
            'Time_Offset': extra.get('offset', '+0000]'),
            'Remote_IP': ip, 'Requester_ARN/Canonical_ID': '-',
            'Operation': extra.get('op', 'REST.GET.OBJECT'), 'Key': key,
            'HTTP_status': extra.get('status', 206 if sent < size else 200),
            'Bytes_Sent': sent, 'Object_Size': float(size),
            'Total_Time': float(extra.get('ms', 100)),
            'User_Agent': extra.get('ua', 'Boto3/1.43.108'),
        })
    return pd.DataFrame(records)


class TestSummariseDownloads(unittest.TestCase):

    def test_grouping(self):
        """Pieces are joined; a later repeat, another file or another IP are new downloads."""
        df = _log([
            ('1.2.3.4', 'spikes.times.npy', '2026-10-01 10:00:00', 40 * MB, 100 * MB),
            ('1.2.3.4', 'spikes.times.npy', '2026-10-01 10:00:01', 30 * MB, 100 * MB),
            ('1.2.3.4', 'spikes.times.npy', '2026-10-01 10:00:01', 30 * MB, 100 * MB),
            ('1.2.3.4', 'spikes.times.npy', '2026-10-01 14:30:00', 100 * MB, 100 * MB),
            ('1.2.3.4', 'clusters.depths.npy', '2026-10-01 10:00:02', 1 * MB, 1 * MB),
            ('5.6.7.8', 'spikes.times.npy', '2026-10-01 10:00:00', 100 * MB, 100 * MB),
        ])
        out = s3io.summarise_downloads(df)
        self.assertEqual(4, len(out))
        self.assertEqual(6, out['n_requests'].sum())
        self.assertEqual(df['Bytes_Sent'].sum(), out['bytes_sent'].sum())
        np.testing.assert_array_equal(out['fraction'], 1.)
        self.assertTrue(out['complete'].all())
        first = out[(out.Remote_IP == '1.2.3.4') & (out.Key == 'spikes.times.npy')].iloc[0]
        self.assertEqual(3, first['n_requests'])
        self.assertEqual(pd.Timestamp('2026-10-01 10:00:00', tz='UTC'), first['start_time'])
        self.assertEqual(pd.Timestamp('2026-10-01 10:00:01.1', tz='UTC'), first['end_time'])
        self.assertTrue(out['start_time'].is_monotonic_increasing)
        self.assertEqual(300, out.attrs['max_gap_s'])

    def test_whole_file_requests(self):
        """Each 200 request is its own download; only 206 pieces are grouped."""
        df = _log([
            # The whole file twice within a minute: two downloads
            ('1.2.3.4', 'a.npy', '2026-10-01 10:00:00', 100, 100),
            ('1.2.3.4', 'a.npy', '2026-10-01 10:00:30', 100, 100),
            # Pieces around a whole-file request: the pieces are one download, the 200 another
            ('1.2.3.4', 'b.npy', '2026-10-01 10:00:00', 50, 100),
            ('1.2.3.4', 'b.npy', '2026-10-01 10:00:01', 100, 100),
            ('1.2.3.4', 'b.npy', '2026-10-01 10:00:02', 50, 100),
        ])
        out = s3io.summarise_downloads(df).sort_values(['Key', 'HTTP_status', 'start_time'])
        self.assertEqual([1, 1, 1, 2], out['n_requests'].tolist())
        self.assertEqual([200, 200, 200, 206], out['HTTP_status'].tolist())
        np.testing.assert_array_equal(out['fraction'], 1.)

    def test_filtering(self):
        """Only successful GET object requests are kept."""
        df = _log([
            ('1.2.3.4', 'a.npy', '2026-10-01 10:00:00', 10, 10),
            ('1.2.3.4', 'b.npy', '2026-10-01 10:00:00', 0, 10, {'op': 'REST.HEAD.OBJECT'}),
            ('1.2.3.4', 'c.npy', '2026-10-01 10:00:00', 0, 10, {'status': 404}),
            ('1.2.3.4', '-', '2026-10-01 10:00:00', 500, 0, {'op': 'REST.GET.BUCKET'}),
        ])
        out = s3io.summarise_downloads(df)
        self.assertEqual(['a.npy'], out['Key'].tolist())

    def test_max_gap(self):
        """The gap is measured from the end of the previous request and can be changed."""
        df = _log([
            ('1.2.3.4', 'a.cbin', '2026-10-01 10:00:00', 50, 100, {'ms': 9.5 * 60e3}),
            ('1.2.3.4', 'a.cbin', '2026-10-01 10:12:00', 50, 100),
        ])
        # 12 min between starts but 2.5 min after the slow first piece ended
        self.assertEqual(1, len(s3io.summarise_downloads(df)))
        self.assertEqual(2, len(s3io.summarise_downloads(df, max_gap=120)))
        # The gap is from the latest end, not just the previous row's end
        df = _log([
            ('1.2.3.4', 'a.cbin', '2026-10-01 10:00:00', 40, 100, {'ms': 20 * 60e3}),
            ('1.2.3.4', 'a.cbin', '2026-10-01 10:01:00', 30, 100),
            ('1.2.3.4', 'a.cbin', '2026-10-01 10:19:00', 30, 100),
        ])
        out = s3io.summarise_downloads(df)
        self.assertEqual(1, len(out))
        self.assertEqual(pd.Timestamp('2026-10-01 10:20:00', tz='UTC'), out['end_time'].iloc[0])

    def test_partial(self):
        df = _log([
            ('1.2.3.4', 'a.npy', '2026-10-01 10:00:00', 65536, 50 * MB,
             {'ua': 'facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)'}),
            ('1.2.3.5', 'a.npy', '2026-10-01 10:00:00', 50 * MB, 50 * MB,
             {'ua': 'Mozilla/5.0 (compatible; Googlebot/2.1)'}),
            ('1.2.3.6', 'a.npy', '2026-10-01 10:00:00', 50 * MB, 50 * MB, {'ua': 'Boto3/1.43.108 botocore/1.40'}),
            ('1.2.3.7', 'a.npy', '2026-10-01 10:00:00', 1, 0),
        ]).sort_values('Remote_IP')
        out = s3io.summarise_downloads(df).set_index('Remote_IP')
        self.assertNotIn('is_bot', out.columns)
        # The pattern for filtering bots in analysis: crawlers, but not Boto3 / botocore
        is_bot = out['User_Agent'].str.contains(s3io.BOT_USER_AGENT_PATTERN, case=False, na=False)
        self.assertEqual([True, True, False, False], is_bot.tolist())
        self.assertFalse(out.loc['1.2.3.4', 'complete'])
        self.assertAlmostEqual(65536 / (50 * MB), out.loc['1.2.3.4', 'fraction'])
        self.assertTrue(np.isnan(out.loc['1.2.3.7', 'fraction']))  # unknown size
        self.assertFalse(out.loc['1.2.3.7', 'complete'])

    def test_time_offset(self):
        df = _log([('1.2.3.4', 'a.npy', '2026-10-01 10:00:00', 10, 10, {'offset': '+0100]'})])
        out = s3io.summarise_downloads(df)
        self.assertEqual(pd.Timestamp('2026-10-01 09:00:00', tz='UTC'), out['start_time'].iloc[0])

    def test_empty(self):
        df = _log([('1.2.3.4', 'a.npy', '2026-10-01 10:00:00', 0, 10, {'status': 404})])
        out = s3io.summarise_downloads(df)
        self.assertTrue(out.empty)
        self.assertIn('fraction', out.columns)



class TestMergeDownloads(unittest.TestCase):

    def setUp(self):
        """A random log of pieces, repeats and slow requests for a few IPs and files."""
        rng = np.random.default_rng(0)
        n = 2000
        t0 = pd.Timestamp('2026-04-01')
        times = t0 + pd.to_timedelta(np.sort(rng.integers(0, 6 * 3600, n)), unit='s')
        # Pieces (206) and some whole-file requests (sent == size, so 200)
        rows = [(f'10.0.0.{rng.integers(5)}', f'file{rng.integers(4)}.npy', t,
                 int(rng.choice([100, *range(1, 50)])), 100,
                 {'ms': float(rng.choice([50, 5e3, 4e5]))})
                for t in times]
        self.log = _log(rows)

    def test_parts_equal_whole(self):
        """Merging the downloads of the parts gives the same table as the whole log."""
        whole = s3io.summarise_downloads(self.log)
        self.assertLess(len(whole), len(self.log))  # some requests were grouped
        self.assertTrue((whole['HTTP_status'] == 200).any())  # including some 200 requests
        self.assertTrue((whole.loc[whole['HTTP_status'] == 200, 'n_requests'] == 1).all())
        self.assertNotIn('ranged', whole.columns)
        for split in ('time', 'random'):
            with self.subTest(split=split):
                if split == 'time':  # e.g. parts consolidated one after another
                    parts = [self.log.iloc[i:i + 700] for i in range(0, len(self.log), 700)]
                else:  # rows of the same download in any part
                    idx = np.random.default_rng(1).integers(0, 4, len(self.log))
                    parts = [self.log[idx == i] for i in range(4)]
                merged = s3io.merge_downloads(s3io.summarise_downloads(p) for p in parts)
                pd.testing.assert_frame_equal(
                    whole.drop(columns=['User_Agent', 'Requester_ARN/Canonical_ID']),
                    merged.drop(columns=['User_Agent', 'Requester_ARN/Canonical_ID']))
                self.assertEqual(300, merged.attrs['max_gap_s'])

    def test_max_gap_checks(self):
        a = s3io.summarise_downloads(self.log[:100], max_gap=300)
        b = s3io.summarise_downloads(self.log[100:], max_gap=60)
        with self.assertRaises(ValueError):
            s3io.merge_downloads([a, b])
        with self.assertRaises(ValueError):
            s3io.merge_downloads([a, a], max_gap=60)
        empty = s3io.summarise_downloads(self.log[:0])
        self.assertTrue(s3io.merge_downloads([empty, empty]).empty)


class TestFindLogTables(unittest.TestCase):
    name = '2026-04_ibl-brain-wide-map-public'

    def _bucket(self, files):
        folder = PurePosixPath(s3io.REMOTE_LOG_LOCATION, 'consolidated')
        keys = [(folder / f).as_posix() for f in files]
        objects = SimpleNamespace(filter=lambda Prefix: [SimpleNamespace(key=k) for k in keys
                                                         if k.startswith(Prefix)])
        return SimpleNamespace(name='bucket', objects=objects), keys

    def _find(self, files):
        bucket, keys = self._bucket(files)
        exists = lambda b, k: PurePosixPath(k).as_posix() in keys
        with mock.patch.object(process, '_s3_object_exists', side_effect=exists):
            return [k.name for k in process._find_log_tables(bucket, self.name)]

    def test_whole_and_parts(self):
        n = self.name
        self.assertEqual([f'{n}.pqt'], self._find([f'{n}.pqt', f'{n}_IP-info.pqt']))
        parts = [f'{n}_PART{i}.pqt' for i in (2, 10, 1, 3, 4, 5, 6, 7, 8, 9)]
        expected = [f'{n}_PART{i}.pqt' for i in range(1, 11)]  # numeric, not text, order
        self.assertEqual(expected, self._find(parts + [f'{n}_IP-info.pqt']))

    def test_errors(self):
        n = self.name
        with self.assertRaisesRegex(RuntimeError, 'missing'):
            self._find([f'{n}_PART1.pqt', f'{n}_PART3.pqt'])
        with self.assertRaisesRegex(RuntimeError, 'Both'):
            self._find([f'{n}.pqt', f'{n}_PART1.pqt'])
        with self.assertRaisesRegex(FileNotFoundError, 'partial'):
            self._find([f'{n}_INCOMPLETE.pqt'])
        with self.assertRaises(FileNotFoundError):
            self._find([])



class TestMonthlyJob(unittest.TestCase):
    """The downloads table is made by the monthly consolidation (the second way in)."""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        patcher = mock.patch.object(s3io, 'LOCAL_LOG_LOCATION', Path(tmp.name))
        patcher.start()
        self.addCleanup(patcher.stop)
        log = _log([
            ('1.2.3.4', 'data/a.npy', '2026-10-01 10:00:00', 40, 100),
            ('1.2.3.4', 'data/a.npy', '2026-10-01 10:00:01', 60, 100),
            ('5.6.7.8', 'data/a.npy', '2026-10-01 11:00:00', 0, 100, {'op': 'REST.HEAD.OBJECT'}),
        ])
        # The remaining columns of a raw log table, as read from the log files (all strings)
        for col in s3io.COL_NAMES:
            if col not in log:
                log[col] = '-'
        log['Bucket_Owner'] = 'a' * 64
        log['Bucket'] = s3io.SOURCE_BUCKET
        # As returned by parse_logs (read_csv with dtype=object)
        self.raw = log[s3io.COL_NAMES].astype(str).astype(object)

    def _consolidate(self, partial, incomplete=None):
        """Run _consolidate_month with S3 replaced by mocks; return the upload_table keys.

        `incomplete` is an optional _INCOMPLETE table already on S3, to be merged in."""
        from botocore.exceptions import ClientError
        not_found = ClientError({'Error': {'Code': '404'}}, 'HeadObject')
        session = mock.MagicMock()
        s3 = session.resource.return_value
        if incomplete is None:
            s3.Object.return_value.load.side_effect = not_found  # no _INCOMPLETE table
        bucket = s3.Bucket.return_value
        bucket.name = 'ibl-brain-wide-map-private'
        listing = mock.MagicMock()  # an empty listing (supports iteration and .delete())
        listing.__iter__.side_effect = lambda: iter([])
        bucket.objects.filter.return_value = listing
        bucket.Object.return_value.load.side_effect = not_found  # no downloads table yet

        def download_file(key, filename):
            if incomplete is not None and key.endswith('_INCOMPLETE.pqt'):
                incomplete.to_parquet(filename)
            else:
                raise not_found  # no IP table yet
        bucket.download_file.side_effect = download_file
        uploaded = {}
        upload = lambda df, key, bucket: uploaded.__setitem__(PurePosixPath(key).name, df)
        start, end = datetime(2026, 10, 1), datetime(2026, 10, 31, 23, 59, 59)
        with mock.patch.object(s3io, 'list_log_keys',
                               return_value=(['logs/2026-10-01-00-00-00-X'], [100])), \
                mock.patch.object(s3io, 'read_remote_logs', return_value=self.raw.copy()), \
                mock.patch.object(s3io, 'upload_table', side_effect=upload), \
                mock.patch.object(s3io, 'delete_logs'), \
                mock.patch.object(process, 'ip_info',
                                  side_effect=lambda ips, **_: [{'ip': ip} for ip in ips]):
            run = {}
            process._consolidate_month(start, end, partial, session, None, 4, run, max_gap=60)
        return uploaded, run

    def test_complete_month(self):
        uploaded, run = self._consolidate(partial=False)
        name = f'2026-10_{s3io.SOURCE_BUCKET}'
        self.assertEqual({f'{name}.pqt', f'{name}_IP-info.pqt', f'{name}_downloads.pqt'},
                         set(uploaded))
        downloads = uploaded[f'{name}_downloads.pqt']
        self.assertEqual(1, len(downloads))  # 2 GET pieces -> 1 download; HEAD dropped
        self.assertEqual(1., downloads['fraction'].iloc[0])
        self.assertEqual(60, downloads.attrs['max_gap_s'])
        self.assertEqual('done', run['stage'])
        self.assertNotIn('error', run)

    def test_host_id_dropped(self):
        """Host_Id is not stored, even when merging an _INCOMPLETE table made with it."""
        self.assertIn('Host_Id', self.raw.columns)  # parsed from the log files
        # An earlier _INCOMPLETE table with Host_Id: one new row and one already in the logs
        incomplete = self.raw.iloc[:2].copy()
        incomplete.loc[incomplete.index[0], 'Request_ID'] = 'EARLIER'
        incomplete = s3io.prepare_for_parquet(incomplete)
        self.assertIn('Host_Id', incomplete.columns)
        uploaded, run = self._consolidate(partial=False, incomplete=incomplete)
        table = uploaded[f'2026-10_{s3io.SOURCE_BUCKET}.pqt']
        # Merged in, and the repeated row still recognised as a duplicate despite the Host_Id
        self.assertEqual(len(self.raw) + 1, len(table))
        self.assertNotIn('Host_Id', table.columns)
        self.assertEqual([c for c in s3io.COL_NAMES if c != 'Host_Id'], list(table.columns))
        self.assertNotIn('error', run)
        # Partial (this_month) tables don't have it either
        uploaded, _ = self._consolidate(partial=True)
        self.assertNotIn('Host_Id', uploaded[f'2026-10_{s3io.SOURCE_BUCKET}_INCOMPLETE.pqt'])

    def test_partial_month(self):
        """Partial (this_month) runs make no downloads table."""
        uploaded, _ = self._consolidate(partial=True)
        self.assertFalse(any(k.endswith('_downloads.pqt') for k in uploaded))

    def test_failure_does_not_stop_consolidation(self):
        with mock.patch.object(s3io, 'summarise_downloads', side_effect=RuntimeError('boom')), \
                self.assertWarnsRegex(UserWarning, 'downloads table failed'):
            uploaded, run = self._consolidate(partial=False)
        self.assertIn(f'2026-10_{s3io.SOURCE_BUCKET}.pqt', uploaded)  # logs still consolidated
        self.assertIn('boom', run['error'])
        self.assertEqual('done', run['stage'])


if __name__ == '__main__':
    unittest.main()

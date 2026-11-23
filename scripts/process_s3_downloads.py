"""
Build a month's downloads table (one row per file download) from its consolidated S3 log table.

The consolidated table is downloaded from the private bucket, successful GET object requests are
kept, requests from the same IP for the same file are grouped into downloads, and the result is
uploaded to <server-access-logs>/downloads/<YYYY-MM>_ibl-brain-wide-map-public_downloads.pqt.

Run from the scripts folder, e.g.

    python process_s3_downloads.py 2026 10
    python process_s3_downloads.py 2026 10 --max-gap 300 --profile ibladmin --overwrite
    python process_s3_downloads.py 2026 10 --source ~/Downloads/2026-10_ibl-brain-wide-map-public.pqt --no-upload

Months whose log table is split into parts (_PART1.pqt, _PART2.pqt, ...) are handled
automatically; with --source, pass all the parts.
"""
import argparse

from s3_logs.process import make_downloads_table

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__.strip().split('\n')[0])
    parser.add_argument('year', type=int)
    parser.add_argument('month', type=int)
    parser.add_argument('--max-gap', type=float, default=300,
                        help='maximum gap in seconds between requests of the same download '
                             '(default: 300)')
    parser.add_argument('--profile', default='ibladmin', help='boto3 profile name (default: ibladmin)')
    parser.add_argument('--source', nargs='+',
                        help='local copy of the consolidated log table (or of all its parts) to '
                             'use instead of downloading it')
    parser.add_argument('--no-upload', action='store_true',
                        help='save the table to ~/s3_logs instead of uploading it')
    parser.add_argument('--overwrite', action='store_true',
                        help='replace the downloads table if it already exists on S3')
    args = parser.parse_args()
    if not 1 <= args.month <= 12:
        parser.error('month must be between 1 and 12')

    _, location = make_downloads_table(
        args.year, args.month, max_gap=args.max_gap, profile_name=args.profile,
        source=args.source, upload=not args.no_upload, overwrite=args.overwrite)
    print(location)

from s3_logs.process import consolidate_logs
import logging

logger = logging.getLogger('ibllib')
logger.setLevel(logging.INFO)

if __name__ == '__main__':

    import sys
    command = sys.argv[1]
    ipinfo_token = sys.argv[2]
    profile_name = 'ibladmin' if len(sys.argv) < 4 else sys.argv[3]
    logger.warning(command)
    logger.warning(ipinfo_token)
    logger.warning(profile_name)
    consolidate_logs(date=command, ipinfo_token=ipinfo_token, profile_name=profile_name)

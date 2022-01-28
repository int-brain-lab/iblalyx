import fastparquet  # required for to_parquet function
import json
import pandas as pd
import re
import time

from datetime import datetime
from os.path import exists

FLAGS = re.VERBOSE | re.MULTILINE | re.DOTALL
WHITESPACE = re.compile(r'[ \t\n\r]*', FLAGS)
START = time.time()


def next_json(s):
    """Takes the largest bite of JSON from the string.
       Returns (object_parsed, remaining_string)
    """
    decoder = json.JSONDecoder()
    obj, end = decoder.raw_decode(s)
    end = WHITESPACE.match(s, end).end()
    return obj, s[end:]


def load_json_log_file(json_file_loc):
    """Loads the JSON file or files from the specified file location

    Parameters
    ----------
    json_file_loc : string
        Location of the JSON file to be evaluated

    Returns
    -------
    pd.DataFrame
        Datasets frame
    """
    # Set variables
    next_log_file_num = 1
    next_log_file_name = json_file_loc
    date_today = datetime.today().isoformat()[:10]
    data = []
    # Check if multiple log file exists or just pull from a single file
    while True:
        if exists(next_log_file_name):
            # Open json file for evaluation
            with open(next_log_file_name) as json_file:
                # Set json data to string
                json_string = json_file.read()
                while len(json_string) > 0:
                    # Read every json chunk
                    obj, remaining = next_json(json_string)
                    # Look at only logs for today
                    if obj['timestamp'][:10] == date_today:
                        # Check if this is the start to a request or a failed request
                        if (obj['event'] == 'request_started') or \
                                (obj['event'] == 'request_failed'):
                            # Parse out request type verb (get, patch, post),
                            # endpoint and full request url
                            parts = obj['request'][1:-1].split()
                            verb = parts[1]
                            url = parts[2][1:-1]
                            endpoint = url.split('/')[1].split('?')[0]
                            # append all info
                            data.append(
                                [obj['level'], obj['ip'], obj['timestamp'], verb, endpoint, url,
                                 obj['request']])

                        # Check if this is a 'duplicate' entry
                        elif obj['event'] == 'request_finished':
                            pass

                        # Encountered something unplanned, output to terminal
                        else:
                            print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                  "> Could not parse -", obj)

                    # Move on to next part of the string
                    json_string = remaining

            # Modify variables for multiple log file check
            next_log_file_name = json_file_loc + "." + str(next_log_file_num)
            next_log_file_num += 1
        else:  # ran out of files to evaluate, break out of loop
            break

    # Create dataframe with appropriate column names
    df = pd.DataFrame(data, columns=[
        'level',
        'ip',
        'datetime',
        'verb_type',
        'endpoint',
        'url',
        'request'])

    return df


if __name__ == '__main__':
    # Set the working directory and file locations
    working_directory = "/var/log/"
    json_file_location = working_directory + "alyx_json.log"
    parquet_out_file_location = working_directory + \
        "alyx_daily_json_parquets/" + time.strftime("%Y-%m-%d") + "_alyx_json_log.parquet"

    # Parse out relevant data from alyx_json file, store to dataframe
    dataframe = load_json_log_file(json_file_location)

    # Output dataframe to parquet file
    dataframe.to_parquet(parquet_out_file_location)

    # Output performance metric of script
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "> Execution time -", (time.time()-START))

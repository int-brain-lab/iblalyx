import json
import re
import time
from datetime import datetime
from os.path import exists

import pandas as pd

FLAGS = re.VERBOSE | re.MULTILINE | re.DOTALL
WHITESPACE = re.compile(r'[ \t\n\r]*', FLAGS)
START = time.time()


def load_simple_log_file(file_location):
    """Loads the simple log file or files from the specified file location

    Parameters
    ----------
    file_location : string
        Location of the simple log file to be evaluated

    Returns
    -------
    pd.DataFrame
        Datasets frame
    """
    # Variables for multi log file evaluation
    next_log_file_num = 1
    next_log_file_name = file_location
    # TODO: Incorporate date check for daily runs
    # date_today = datetime.today().isoformat()[:10]
    data = []
    # Check if multiple log file exists or just pull from a single file
    while True:
        if exists(next_log_file_name):
            # Open json file for evaluation
            with open(next_log_file_name) as file:
                line_number = 0
                for line in file:
                    # DEBUG ASSIST
                    line_number += 1
                    print(line_number)

                    # Skip line if it is a 'request_finished'
                    if 'request_finished' in line:
                        continue

                    # Variables to set
                    my_datetime = ''
                    ip = ''
                    verb_type = ''
                    endpoint = ''
                    url = ''
                    request = ''

                    level = re.search(r'\[INFO]|\[WARNING]', line).group(0)
                    if level == '[WARNING]':
                        my_datetime = re.search(r'\d{2}/\d{2} \d{2}:\d{2}:\d{2}', line).group(0)
                        my_datetime = '2022-' + my_datetime[3:5] + '-' + my_datetime[0:2] + 'T' + \
                                      my_datetime[6:] + 'Z'
                        ip = 'N/A'
                        verb_type = 'N/A'
                        endpoint = 'N/A'
                        url = 'N/A'
                        request = line[line.find('{'):-5]

                    elif level == '[INFO]':
                        # check if this is not a typical request
                        if 'request' not in line:
                            my_datetime = re.search(r'\d{2}/\d{2} \d{2}:\d{2}:\d{2}',
                                                    line).group(0)
                            my_datetime = '2022-' + my_datetime[3:5] + '-' + my_datetime[0:2] + \
                                          'T' + my_datetime[6:] + 'Z'
                            ip = 'N/A'
                            verb_type = 'N/A'
                            endpoint = 'N/A'
                            url = 'N/A'
                            # request_index = line.find('{')
                            request = line[line.find('{'):-5]

                        else:
                            # Regular expression to find variables
                            my_datetime = re.search(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{6}Z',
                                                    line).group(0)
                            ip = re.search(r'[0-9]+(?:\.[0-9]+){3}', line).group(0)
                            verb_type = re.search(r'GET|POST|PATCH', line).group(0)

                            # Break line into individual components to parse challenging data
                            partials = line.split()
                            endpoint = ''
                            url = ''
                            request_index = 0
                            for part in partials:
                                # Find endpoint and url
                                if '?' in part:
                                    endpoint = part.split('/')[1].split('?')[0]
                                    url = part.split('\'')[1]
                                elif ('/' in part) and ('>' in part):
                                    if '\'>,' in part:
                                        endpoint = part.split('/')[1].replace('\'>,', '')
                                        url = part.replace('\'', '').replace('>,', '')
                                    else:
                                        endpoint = part.split('/')[1]

                                # Extract the request_index
                                elif '<WSGI' in part:
                                    request_index = partials.index(part)

                            # Build request from the last end of the string
                            request = (partials[request_index] + partials[request_index+1] +
                                       partials[request_index+2])[:-1]

                    # Unanticipated log entry
                    else:
                        print("Unanticipated log entry:", line)

                    # Store parsed data to data list
                    data.append({
                        'level': level,
                        'ip': ip,
                        'datetime': my_datetime,
                        'verb_type': verb_type,
                        'endpoint': endpoint,
                        'url': url,
                        'request': request
                    })

            # Modify variables for multiple log file check
            next_log_file_name = file_location + "." + str(next_log_file_num)
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


def next_json(s):
    """Takes the largest bite of JSON from the string.
       Returns (object_parsed, remaining_string)
    """
    decoder = json.JSONDecoder()
    obj, end = decoder.raw_decode(s)
    end = WHITESPACE.match(s, end).end()
    return obj, s[end:]


def load_json_log_file(file_location):
    """Loads the JSON file or files from the specified file location

    Parameters
    ----------
    file_location : string
        Location of the JSON file to be evaluated

    Returns
    -------
    pd.DataFrame
        Datasets frame
    """
    # Set variables
    next_log_file_num = 1
    next_log_file_name = file_location
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
                                [obj['level'], obj['ip'], obj['timestamp'], verb, endpoint,
                                 url,
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
            next_log_file_name = file_location + "." + str(next_log_file_num)
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
    simple_file_location = working_directory + "alyx_simple.log"
    json_file_location = working_directory + "alyx_json.log"
    parquet_out_file_location = working_directory + \
        "alyx_daily_parquets/" + time.strftime("%Y-%m-%d")
    simple_parquet_out_file = parquet_out_file_location + "_alyx_simple_log.parquet"
    json_parquet_out_file = parquet_out_file_location + "_alyx_json_log.parquet"

    # Parse out relevant data from simple alyx.log file, store to dataframe
    simple_dataframe = load_simple_log_file(simple_file_location)

    # Output simple dataframe to parquet file
    simple_dataframe.to_parquet(simple_parquet_out_file)

    # Parse out relevant data from alyx_json file, store to dataframe
    json_dataframe = load_json_log_file(json_file_location)

    # Output json dataframe to parquet file
    json_dataframe.to_parquet(json_parquet_out_file)

    # Print performance metric of script
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "> Execution time -",
          str(time.time()-START)[:4] + " seconds")

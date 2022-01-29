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
    # Set variables
    next_log_file_num = 1
    next_log_file_name = file_location
    date_today = datetime.today().isoformat()[:10]
    data = []
    # Check if multiple log file exists or just pull from a single file
    while True:
        if exists(next_log_file_name):
            # Open json file for evaluation
            with open(next_log_file_name) as file:
                for line in file:
                    my_dict = {
                        'level': '',
                        'ip': ''
                    }
                    for part in line.split():
                        if part == '[INFO]':
                            my_dict['level'] = part
                            print()
                    data.append(my_dict)

            # [37m29/01 12:40:01 [INFO]
            # {request.py:55}
            # {'request_id': '5b28a112-e476-435d-9bee-202276e8ca4f',
            # 'user_id': None,
            # 'ip': '128.112.219.86', 'request': <WSGIRequest: GET '/sessions?django=&date_range=2021-11-09%2C2021-11-09&number=001&subject=fip_22'>, 'user_agent': 'python-requests/2.26.0', 'event': 'request_started', 'timestamp': '2022-01-29T12:40:01.448810Z', 'level': 'info'}

            # Modify variables for multiple log file check
            next_log_file_name = file_location + "." + str(next_log_file_num)
            next_log_file_num += 1
        else:  # ran out of files to evaluate, break out of loop
            break
    # Code below left in case we are going to be looking to parse out access_alyx-main.log
    # Loading of access_alyx-main log
    # file_location = "/home/user/Documents/IBL/access_alyx-main.log"
    # df = pd.DataFrame(columns=[
    #     'ip',
    #     'datetime',
    #     'verb_type',
    #     'endpoint',
    #     'url'])
    # with open(file_location) as file:
    #     for line in file.readlines():
    #         parsed_ip_address = ""
    #         parsed_datetime = ""
    #         parsed_verb_type = ""
    #         parsed_endpoint = ""
    #         parsed_url = ""
    #         parsed_list = []
    #         for part in line.split():
    #             try:
    #                 # Get IP Address
    #                 if not parsed_ip_address:
    #                     parsed_ip_address = ipaddress.ip_address(part).compressed
    #                     parsed_list.append(parsed_ip_address)
    #                 # Get datetime
    #                 elif not parsed_datetime:
    #                     parsed_datetime = datetime.strptime(
    #                         part, "[%d/%b/%Y:%X").strftime("%Y-%m-%d:%X")
    #                     parsed_list.append(parsed_datetime)
    #                 # Get verb type
    #                 elif not parsed_verb_type:
    #                     if '"GET' in part:
    #                         parsed_verb_type = 'GET'
    #                     elif '"PATCH' in part:
    #                         parsed_verb_type = 'PATCH'
    #                     elif '"POST' in part:
    #                         parsed_verb_type = 'POST'
    #                     if parsed_verb_type:
    #                         parsed_list.append(parsed_verb_type)
    #                 # Get parsed_endpoint
    #                 elif not parsed_endpoint:
    #                     if part.startswith('/'):
    #                         parsed_endpoint = part
    #                         parsed_list.append(parsed_endpoint)
    #                 # Get parsed_url
    #                 elif not parsed_url:
    #                     if part.startswith('"https://'):
    #                         parsed_url = part
    #                         parsed_list.append(parsed_url)
    #             except ValueError:
    #                 pass
    #         # end of part for loop
    #         if not parsed_url:
    #             # URL was never set b/c log likely does not include it
    #             parsed_url = "N/A"
    #             parsed_list.append(parsed_url)
    #
    #         row = pd.Series(parsed_list, index=df.columns)
    #         print(row)
    #         df = df.append(row, ignore_index=True)
    #
    #         # df.loc[len(df)] = parsed_list
    #         # newDF.append(pd.Series(new_row, index=newDF.columns[:len(new_row)]), ignore_index=True)
    #         # df.append(
    #         #     pd.Series(parsed_list, index=df.columns[:len(parsed_list)]), ignore_index=True)
    #
    # # Save to the data frame/series?
    # # for key, value in parser_dict.items():
    # #     print(key, ':', value)
    # # print(df)
    # #
    # print(df)

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
    simple_file_location = working_directory + "partial_alyx_simple.log"
    json_file_location = working_directory + "partial_alyx_json.log"
    parquet_out_file_location = working_directory + \
        "alyx_daily_parquets/" + time.strftime("%Y-%m-%d")
    simple_parquet_out_file = parquet_out_file_location + "_alyx_simple_log.parquet"
    json_parquet_out_file = parquet_out_file_location + "_alyx_json_log.parquet"

    # Parse out relevant data from simple alyx.log file, store to dataframe
    simple_dataframe = load_simple_log_file(simple_file_location)

    # Output simple dataframe to parquet file
    # simple_dataframe.to_parquet(simple_parquet_out_file)

    # Parse out relevant data from alyx_json file, store to dataframe
    # json_dataframe = load_json_log_file(json_file_location)

    # Output json dataframe to parquet file
    # json_dataframe.to_parquet(json_parquet_out_file)

    # Print performance metric of script
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "> Execution time -",
          str(time.time()-START)[:4] + " seconds")


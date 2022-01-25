import re
import json
import pandas as pd


# TODO:
#   - output as csv file

FLAGS = re.VERBOSE | re.MULTILINE | re.DOTALL
WHITESPACE = re.compile(r'[ \t\n\r]*', FLAGS)


def next_json(s):
    """Takes the largest bite of JSON from the string.
       Returns (object_parsed, remaining_string)
    """
    decoder = json.JSONDecoder()
    obj, end = decoder.raw_decode(s)
    end = WHITESPACE.match(s, end).end()
    return obj, s[end:]


def load_json_log_file(json_file_location):

    # Open json file for evaluation
    with open(json_file_location) as json_file:
        json_string = json_file.read()

    # Read every json chunk
    data = []
    while len(json_string) > 0:
        # Read the next json chunk
        obj, remaining = next_json(json_string)
        # Check if this is a 'duplicate' entry
        if obj['event'] == 'request_started':
            # Parse out request type verb (get, patch, post), endpoint and full request url
            parts = obj['request'][1:-1].split()
            verb = parts[1]
            url = parts[2][1:-1]
            endpoint = url.split('/')[1].split('?')[0]
            # append all info
            data.append([obj['level'], obj['ip'], obj['timestamp'], verb, endpoint, url, obj['request']])

        # Check if this is a 'duplicate' entry
        elif obj['event'] == 'request_finished':
            pass
        else:
            print("unknown event", obj)
        # Move on to next part of the string
        json_string = remaining

    # Add in url column once it becomes available in json
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
    # Loading alyx_json file
    # file_location = "/home/user/Documents/IBL/alyx_json_partial.log"
    file_location = "/var/log/alyx_json.log"
    load_json_log_file(file_location)

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

import json
import pandas as pd


# TODO:
#   - Assume JSON file will be modified to have header called 'log'
#       (potentially we can just script the addition of the header and trailing commas on each
#       line)
#   - store as dataframe
#   - output as csv file
#   - deduplicate requests

def load_json_log_file(json_file_location):
    # Add in url column once it becomes available in json
    df = pd.DataFrame(columns=[
        'ip',
        'datetime',
        'verb_type',
        'endpoint'])

    # Open json file for evaluation
    with open(json_file_location) as json_file:
        data = json.load(json_file)

        # Read every
        for i in data['log']:
            # Check if this is a 'duplicate' entry
            if i['event'] == 'request_started':
                # Set IP Address and timestamp
                parsed_list = [i['ip'], i['timestamp']]

                # Parse out request type verb (get, patch, post) and endpoint
                for part in i['request'].split():
                    if part == 'GET':
                        parsed_list.append('GET')
                    elif part == 'PATCH':
                        parsed_list.append('PATCH')
                    elif part == 'POST':
                        parsed_list.append('POST')
                    elif part.startswith('\'/'):
                        parsed_list.append(part[1:len(part)-2])

                # Placeholder for extracting URL information

                # Create pandas series
                row = pd.Series(parsed_list, index=df.columns)
                df = df.append(row, ignore_index=True)

            # Check if this is a 'duplicate' entry
            elif i['event'] == 'request_finished':
                pass
            else:
                print("unknown event", i)


if __name__ == '__main__':
    # Loading alyx_json file
    file_location = "/home/user/Documents/IBL/alyx_json_partial.log"
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

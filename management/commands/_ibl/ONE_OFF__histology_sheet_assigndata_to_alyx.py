'''
One off script to populate Alyx based on G-sheet where we assigned histology
'''
from experiments.models import ProbeInsertion
from pathlib import Path
from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import client, file, tools
import pandas as pd

# Define paths to authentication details
credentials_file_path = Path.home().joinpath('.google', 'credentials.json')
clientsecret_file_path = Path.home().joinpath('.google', 'client_secret.json')

SCOPE = ['https://www.googleapis.com/auth/spreadsheets']
# See if credentials exist
store = file.Storage(credentials_file_path)
credentials = store.get()
# If not get new credentials
if not credentials or credentials.invalid:
    flow = client.flow_from_clientsecrets(clientsecret_file_path, SCOPE)
    credentials = tools.run_flow(flow, store)

# Test getting data from sheet
drive_service = build('drive', 'v3', http=credentials.authorize(Http()))
sheets = build('sheets', 'v4', http=credentials.authorize(Http()))
read_spreadsheetID = '1nidCu7MjLrjaA8NHWYnJavLzQBjILZhCkYt0OdCUTxg'
read_spreadsheetRange = 'NEW_3TEST'
rows = sheets.spreadsheets().values().get(spreadsheetId=read_spreadsheetID,
                                          range=read_spreadsheetRange).execute()

data_sheet = pd.DataFrame(rows.get('values'))
data_sheet = data_sheet.rename(columns=data_sheet.iloc[0]).drop(data_sheet.index[0]).reset_index(drop=True)
data_sheet = data_sheet[data_sheet['sess_id'] != ""]

for pid, lab_assigned in zip(data_sheet.ins_id, data_sheet.assign_lab):
    pi = ProbeInsertion.objects.filter(id=pid)
    # Get json and change it
    d = pi.json
    d['todo_alignment'] = lab_assigned
    pi.json = d
    pi.save()

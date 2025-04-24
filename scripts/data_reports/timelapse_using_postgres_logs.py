# Your data as a string (replace this with reading from a file if it's in a file)
# data = """
#    oid    |    table_schema    |              table_name              | row_estimate | total_bytes | index_bytes | toast_bytes | table_bytes |   total    |   index    |   toast    |   table
# ----------+--------------------+--------------------------------------+--------------+-------------+-------------+-------------+-------------+------------+------------+------------+------------
#      2619 | pg_catalog         | pg_statistic                         |          980 |     1892352 |       73728 |      761856 |     1056768 | 1848 kB    | 72 kB      | 744 kB     | 1032 kB
#      1247 | pg_catalog         | pg_type                              |          684 |      319488 |       98304 |        8192 |      212992 | 312 kB     | 96 kB      | 8192 bytes | 208 kB
#      3118 | pg_catalog         | pg_foreign_table                     |            0 |       16384 |        8192 |        8192 |           0 | 16 kB      | 8192 bytes | 8192 bytes | 0 bytes
# """

# %%
import pandas as pd
from datetime import datetime
from pathlib import Path
import tqdm

import pandas as pd

TABLES = ["actions_session", "subjects_subject", "data_dataset"]

folder_tables = Path('/backups/alyx-backups/table_sizes')  # 2025-04-24_pg_stats.txt
all_dataframes = []

for file_path in tqdm.tqdm(folder_tables.glob('*_pg_stats.txt')):
    # Extract date from file name
    date_str = file_path.name.split('_')[0]
    date = datetime.strptime(date_str, '%Y-%m-%d').date()

    # Read the data into a DataFrame
    df = pd.read_csv(file_path,
                     sep='|',
                     skipinitialspace=True,
                     skiprows=2,  # Skip the header row
                     names=['oid', 'table_schema', 'table_name', 'row_estimate', 'total_bytes',
                            'index_bytes', 'toast_bytes', 'table_bytes', 'total', 'index',
                            'toast', 'table'])

    # Strip whitespace from string columns
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    # Convert numeric columns to appropriate types
    numeric_columns = ['oid', 'row_estimate', 'total_bytes', 'index_bytes', 'toast_bytes', 'table_bytes']
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')

    # Filter the DataFrame to include only the specified tables
    df = df.loc[df['table_name'].isin(TABLES)]

    # Add the date column
    df['date'] = date
    all_dataframes.append(df)


df = pd.concat(all_dataframes)
df.to_parquet(folder_tables.parent.joinpath('table_sizes.pqt'))

# %% On a machine with graphical interface
# scp mbox:/backups/alyx-backups/table_sizes.pqt ~/Downloads/
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from ibl_style.style import figure_style
import numpy as np
import addcopyfighandler
df = pd.read_parquet('~/Downloads/table_sizes.pqt')
figure_style()


s = {'ephys': 150.062254512266,
 'video': 91.17344583453541,
 'extracted': 14.164533004191071,
 'experimental': 71.56434932810953
}


# Filter the dataframe to include only subjects and sessions
df_filtered = df[df['table_name'].isin(['subjects_subject', 'actions_session'])]
ibad = np.logical_and(df_filtered['table_name'] == 'actions_session', df_filtered['row_estimate'] < 45_000)
df_filtered.loc[ibad, 'row_estimate'] = np.nan


# Create the plot
fig, axs = plt.subplots(nrows=1, ncols=2, figsize=(9, 4), gridspec_kw={'width_ratios': [1.5, 1]})

data = s.values()
labels = s.keys()
explode = [0.1 for d in data]
#define Seaborn color palette to use
colors = sns.color_palette('pastel')[0:5]
#create pie chart
# plt.pie(data, labels = labels, colors = colors, autopct='%.0f%%')
axs[1].set(title=f"{int(sum(s.values()))} terabytes of data")
axs[1].pie(data, colors = colors, autopct='%.0f%%', explode=explode)
axs[1].legend(labels=labels)




ax1 = axs[0]
# Get colors from the default matplotlib palette
color1 = plt.cm.tab10(0)
color2 = plt.cm.tab10(1)

# Plot subjects on the left y-axis
sns.lineplot(data=df_filtered[df_filtered['table_name'] == 'subjects_subject'],
             x='date', y='row_estimate', ax=ax1, color=color1, label='Subjects')
ax1.set_ylabel('Number of Subjects', color=color1)
ax1.tick_params(axis='y', labelcolor=color1)

# Create a twin axis for sessions
ax2 = ax1.twinx()

ddf = df_filtered.loc[df_filtered['table_name'] == 'actions_session'].copy()
# Plot sessions on the right y-axis
sns.lineplot(x=ddf['date'].values, y=ddf['row_estimate'].values, ax=ax2, color=color2, label='Sessions')
ax2.set_ylabel('Number of Sessions', color=color2)
ax2.tick_params(axis='y', labelcolor=color2)

# Set the title and x-axis label
plt.title('Subjects and Sessions Over Time')
ax1.set_xlabel('Date')

# Combine legends
lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
ax1.tick_params(axis='x', rotation=45)

# Remove the automatic legend from seaborn
ax1.get_legend().remove()
ax2.get_legend().remove()

# Adjust layout to prevent cutting off labels
plt.tight_layout()

# Show the plot
plt.show()

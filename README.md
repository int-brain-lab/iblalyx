# iblalyx

The `scripts` folder contains one offs, or manual scripts to perform specific interventions or queries and report.

The `management` folder is meant to populate an alyx django application to be used within the scope of the manage commands.
In practice a symlink sends the ibl.py file and the private package _ibl to the alyx app via the following command on the server:

```
ROOT_APP=~/iblalyx
ROOT_ALYX=/var/www/alyx-main/alyx

ln -s $ROOT_APP/management/commands/_ibl $ROOT_ALYX/data/management/commands/_ibl
ln -s  $ROOT_APP/management/commands/ibl.py $ROOT_ALYX/data/management/commands/ibl.py
ln -s  $ROOT_APP/management/commands/sync_patcher.py $ROOT_ALYX/data/management/commands/sync_patcher.py

# to link the ibl_reports page
ln -s  $ROOT_APP/management/ibl_reports $ROOT_ALYX/ibl_reports
ln -s  $ROOT_APP/management/ibl_reports/templates $ROOT_ALYX//templates/ibl_reports
```

## Commands

### histology_assign_update
- Randomly assign ephys insertions to an experimenter to do the histology alignment (the algorithm will assign in priority the experimenter with the least N alignment to do).
- Check whether the insertion has histology alignment done, whether it is critical and contains all datasets required.
- Update the G-Sheet: https://docs.google.com/spreadsheets/d/1nidCu7MjLrjaA8NHWYnJavLzQBjILZhCkYt0OdCUTxg/edit#gid=1160679914

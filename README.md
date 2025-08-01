# iblalyx

## Alyx IBL plugin

The `management` folder is meant to populate an alyx django application to be used within the scope of the manage commands.
In practice a symlink sends the ibl.py file and the private package _ibl to the alyx app via the following command on the server:

### Installation

```
# those are the roots of repositories
IBL_ALYX_ROOT=/home/datauser/Documents/PYTHON/iblalyx
ALYX_ROOT=/home/datauser/Documents/PYTHON/alyx

ln -s $IBL_ALYX_ROOT/management/commands/_ibl $ALYX_ROOT/alyx/data/management/commands/_ibl
ln -s $IBL_ALYX_ROOT/management/commands/ibl.py $ALYX_ROOT/alyx/data/management/commands/ibl.py
ln -s $IBL_ALYX_ROOT/management/commands/sync_patcher.py $ALYX_ROOT/alyx/data/management/commands/sync_patcher.py
ln -s $IBL_ALYX_ROOT/management/commands/create_public_links.py $ALYX_ROOT/alyx/data/management/commands/create_public_links.py

# to link the ibl_reports page
ln -s  $IBL_ALYX_ROOT/management/ibl_reports $ALYX_ROOT/alyx/ibl_reports
ln -s  $IBL_ALYX_ROOT/management/ibl_reports/templates $ALYX_ROOT/alyx/templates/ibl_reports
```

### Commands: histology_assign_update
- Randomly assign ephys insertions to an experimenter to do the histology alignment (the algorithm will assign in priority the experimenter with the least N alignment to do).
- Check whether the insertion has histology alignment done, whether it is critical and contains all datasets required.
- Update the G-Sheet: https://docs.google.com/spreadsheets/d/1nidCu7MjLrjaA8NHWYnJavLzQBjILZhCkYt0OdCUTxg/edit#gid=1160679914

## Deployment and scripts

The `scripts` folder contains one offs, or manual scripts to perform specific interventions or queries and report.

The `cron` folder contains periodic tasks that are run mostly on mbox, but also on SDSC.

## Releases

The `releases` folder keeps a log of the official IBL data releases.
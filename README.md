# iblalyx

The `scripts` folder contains one offs, or manual scripts to perform specific interventions or queries and report.

The `management` folder is meant to populate an alyx django application to be used within the scope of the manage commands.
In practice a symlink sends the ibl.py file and the private package _ibl to the alyx app via the following command on the server:

```
ln -s ~/iblalyx/management/commands/_ibl /var/www/alyx-main/alyx/data/management/commands/_ibl
ln -s ~/iblalyx/management/commands/ibl.py /var/www/alyx-main/alyx/data/management/commands/ibl.py
```

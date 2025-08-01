# Alyx Data Release Tools

This folder contains scripts and utilities for managing data releases from the internal Alyx database to the public OpenAlyx instance.
It also contains the full history of releases on the public openalyx database.

The full release instructions are available at [https://dev.internationalbrainlab.org/data_release.html](https://dev.internationalbrainlab.org/data_release.html).


## Roadmap:
As the set of public datasets grows, the process of doing full releases is more and more demanding. As of August 2025 it takes around 20 Gb of RAM to perform the pruning of the production database.
There may be a better way to paginate or speed up the process, especially in `01a_prune_public_db.py`.

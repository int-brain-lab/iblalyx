# this code is meant to run on the SDSC flatiron instance
from pathlib import Path
from oneibl.one import ONE
from oneibl.webclient import sdsc_path_from_dataset, SDSC_ROOT_PATH

SDSC_PUBLIC_PATH = Path('/mnt/ibl/public')

one = ONE(base_url='https://openalyx.internationalbrainlab.org', username='intbrainlab', password='international')
dsets = one.alyx.rest('datasets', 'list')


# THis won't work anymore as the sdsc_path_from_dataset(dset) will return the public path with the data repositories url and Gloub path setup
# for dset in dsets:
#     dpath = sdsc_path_from_dataset(dset)
#     link_path = SDSC_PUBLIC_PATH.joinpath(dpath.relative_to(SDSC_ROOT_PATH))
#     if link_path.exists():
#         print(f"skipped existing {link_path}")
#         continue
#     link_path.parent.mkdir(exist_ok=True, parents=True)
#     link_path.symlink_to(dpath)
#     print(f"linked {link_path}")

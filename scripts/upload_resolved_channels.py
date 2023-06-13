from ibllib.pipes.tasks import Task
from ibllib.pipes.ephys_alignment import EphysAlignment
import spikeglx
from ibllib.atlas import AllenAtlas
import one.alf.io as alfio
import numpy as np
from one.api import ONE
from experiments.models import ProbeInsertion
from django.db.models import Count, Q
import logging

logger = logging.getLogger('ibllib')
logger.setLevel(logging.INFO)

ba = AllenAtlas()
one = ONE()


class RegisterChannels(Task):

    @property
    def signature(self):
        signature = {
            'input_files': [('channels.localCoordinates.npy',
                             f'alf/{self.pname}*', False),
                            ('*.ap.meta', f'raw_ephys_data/{self.pname}*',
                             False)],
            'output_files': []
        }
        return signature

    def __init__(self, session_path, pname=None, **kwargs):
        super().__init__(session_path, **kwargs)

        self.pname = pname
        self.eid = self.one.path2eid(session_path)

    def _run(self):
        ins = self.one.alyx.rest('insertions', 'list', session=self.eid,
                                 name=self.pname)[0]
        xyz = np.array(ins['json']['xyz_picks']) / 1e6
        traj = self.one.alyx.rest('trajectories', 'list',
                                  probe_insertion=ins['id'],
                                  provenance='Ephys aligned histology track')[0]
        align_key = ins['json']['extended_qc']['alignment_stored']
        feature = traj['json'][align_key][0]
        track = traj['json'][align_key][1]

        files_to_register = []

        collections = self.one.list_collections(self.eid, filename='spikes*',
                                                collection=f'alf/{self.pname}*')

        # loop over all spike sorting collections and generate
        # the channel extra files for them
        for collection in collections:
            alf_path = self.session_path.joinpath(collection)
            chans = alfio.load_object(alf_path, 'channels')
            depths = chans.localCoordinates[:, 1]
            ephysalign = EphysAlignment(xyz, depths, track_prev=track,
                                        feature_prev=feature,
                                        brain_atlas=ba, speedy=True)

            channels_mlapdv = np.int32(
                ephysalign.get_channel_locations(feature, track) * 1e6)
            channels_brainID = ephysalign.get_brain_locations(
                channels_mlapdv / 1e6)['id']

            f_name = alf_path.joinpath('channels.mlapdv.npy')
            np.save(f_name, channels_mlapdv)
            files_to_register.append(f_name)

            # Make the channels.brainLocationIds dataset
            f_name = alf_path.joinpath('channels.brainLocationIds_ccf_2017.npy')
            np.save(f_name, channels_brainID)
            files_to_register.append(f_name)

        # generate the histology files

        meta_data_file = next(self.session_path.joinpath(
            'raw_ephys_data', self.pname).glob('*ap.meta'))

        geometry = spikeglx._geometry_from_meta(
            spikeglx.read_meta_data(meta_data_file))

        ephysalign = EphysAlignment(xyz, geometry['y'], track_prev=track,
                                    feature_prev=feature,
                                    brain_atlas=ba, speedy=True)
        channels_mlapdv = np.int32(
            ephysalign.get_channel_locations(feature, track) * 1e6)
        channels_brainID = ephysalign.get_brain_locations(
            channels_mlapdv / 1e6)['id']

        alf_path = self.session_path.joinpath('alf', self.pname)
        f_name = alf_path.joinpath('electrodeSites.mlapdv.npy')
        np.save(f_name, channels_mlapdv)
        files_to_register.append(f_name)

        f_name = alf_path.joinpath('electrodeSites.brainLocationIds_ccf_2017.npy')
        np.save(f_name, channels_brainID)
        files_to_register.append(f_name)

        f_name = alf_path.joinpath('electrodeSites.localCoordinates.npy')
        np.save(f_name, np.c_[geometry['x'], geometry['y']])
        files_to_register.append(f_name)

        return files_to_register


ins = ProbeInsertion.objects.filter(json__extended_qc__alignment_resolved=True)
ins = ins.annotate(n_chns=Count("datasets", filter=Q(
    datasets__name='channels.mlapdv.npy')))
ins = ins.filter(n_chns__lt=1)

logger.info(f'Uploading channels for {ins.count()} insertions')

for pr in ins[0]:
    try:
        traj = list(pr.trajectory_estimate.order_by('-provenance').all()[0].json.keys())
        traj.sort(reverse=True)
        align_stored = pr.json['extended_qc']['alignment_stored']
        if traj[0] != align_stored:
            logger.warning(f'{pr.id} key mismatch: {align_stored} {traj}')
            # Output to a logger
            traj_align = next(t for t in traj if t.split('_')[1] in align_stored)
            logger.info(f'Updating key for {pr.id} to {traj_align}')
            pr.json['extended_qc']['alignment_stored'] = traj_align
            pr.save()

        eid, name = one.pid2eid(pr.id)
        session_path = one.eid2path(eid)

        task = RegisterChannels(session_path, one=one, pname=name, location='SDSC')
        task.run()
        response = task.register_datasets()
        task.cleanUp()
    except Exception as err:
        logger.error(f'{pr.id} errored with message: {err}')
        task.cleanUp()

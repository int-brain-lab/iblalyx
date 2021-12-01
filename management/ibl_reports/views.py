import django_filters
from django.http import HttpResponse, JsonResponse
from django.template import loader
from django.views.generic.list import ListView
from django.db.models import Count, Q, F, Max, OuterRef, Exists
from django.db.models.functions import Coalesce
from django.contrib.auth.mixins import LoginRequiredMixin

from data.models import Dataset
from experiments.models import TrajectoryEstimate, ProbeInsertion
from misc.models import Note, Lab
from subjects.models import Project
from actions.models import Session

import numpy as np
from ibl_reports import qc_check
from ibl_reports import data_check

LOGIN_URL = '/admin/login/'


class SessionTaskView():
    pass

class SessionImportantPlots(LoginRequiredMixin, ListView):
    template_name = 'ibl_reports/gallery_session_overview.html'
    login_url = LOGIN_URL
    paginate_by = 20

    def get_context_data(self, **kwargs):
        # need to figure out which is more efficient
        context = super(SessionImportantPlots, self).get_context_data(**kwargs)
        notes = Note.objects.all().filter(Q(text__icontains='Single-frame camera') | Q(text__icontains='Drift map'))
        s, data = self.get_my_data(context['object_list'], notes)

        context['info'] = data
        context['sessions'] = s

        #qs = qs.annotate(
        #    ephys_probe0=Note.objects.filter(object_id=OuterRef('probe_insertion__pk'), content_object__name='probe00',
        #                                     text__icontains='Drift map').values('id'))
        return context

    def get_my_data(self, sessions, notes):
        important_session_plot = ['Single-frame camera',
                                  'Single-frame camera 1']
        important_probe_plot = ['Drift map',
                                'Drift map 2',
                                'Drift map 3']

        data = []
        s = []
        for sess in sessions:
            info = {}
            s.append(sess)
            plot_dict = {}
            for plot in important_session_plot:
                plot_dict[plot] = notes.filter(object_id=sess.id, text__icontains=plot).first()
            info['session'] = plot_dict
            probes = sess.probe_insertion.values()
            if probes.count() > 0:
                for probe in probes:
                    la = probe
                    plot_dict = {}
                    for plot in important_probe_plot:
                        plot_dict[plot] = notes.filter(object_id=probe['id'], text__icontains=plot).first()
                    info[probe['name']] = plot_dict
            data.append(info)


        return s, data

    def get_queryset(self):
        qs = Session.objects.filter(task_protocol__icontains='ephysChoiceWorld').prefetch_related('probe_insertion')

        return qs.order_by('-session__start_time')


class InsertionOverview(LoginRequiredMixin, ListView):
    template_name = 'ibl_reports/plots.html'
    login_url = LOGIN_URL

    def get_context_data(self, **kwargs):
        context = super(InsertionOverview, self).get_context_data(**kwargs)
        pid = self.kwargs.get('pid', None)
        probe = ProbeInsertion.objects.get(id=pid)
        dsets = probe.session.data_dataset_session_related

        context['probe'] = probe
        context['data'] = {}
        context['data']['raw_behaviour'] = data_check.raw_behaviour_data_status(dsets, probe)
        context['data']['raw_passive'] = data_check.raw_passive_data_status(dsets, probe)
        context['data']['raw_ephys'] = data_check.raw_ephys_data_status(dsets, probe)
        context['data']['raw_video'] = data_check.raw_video_data_status(dsets, probe)
        context['data']['trials'] = data_check.trial_data_status(dsets, probe)
        context['data']['wheel'] = data_check.wheel_data_status(dsets, probe)
        context['data']['passive'] = data_check.passive_data_status(dsets, probe)
        context['data']['ephys'] = data_check.ephys_data_status(dsets, probe)
        context['data']['spikesort'] = data_check.spikesort_data_status(dsets, probe)
        context['data']['video'] = data_check.video_data_status(dsets, probe)
        context['data']['dlc'] = data_check.dlc_data_status(dsets, probe)

        return context

    def get_queryset(self):
        lab = self.kwargs.get('lab', None)
        qs = Lab.objects.all().filter(name=lab)

        return qs


def plot_task_qc(request, pid):
    extended_qc = ProbeInsertion.objects.get(id=pid).session.extended_qc
    # TODO fix error when the extended qc is None
    task = {key: val for key, val in extended_qc.items() if '_task_' in key}
    col, bord = qc_check.get_task_qc_colours(task)

    return JsonResponse({
        'title': f'Task QC: {extended_qc.get("task", "Not computed")}',
        'data': {
            'labels': list(task.keys()),
            'datasets': [{
                'backgroundColor': col,
                'borderColor': bord,
                'borderWidth': 3,
                'data': list(task.values()),
            }]
        },
    })

def plot_task_qc_eid(request, eid):
    extended_qc = Session.objects.get(id=eid).extended_qc
    # TODO fix error when the extended qc is None
    task = {key: val for key, val in extended_qc.items() if '_task_' in key}
    col, bord = qc_check.get_task_qc_colours(task)

    return JsonResponse({
        'title': f'Task QC: {extended_qc.get("task", "Not computed")}',
        'data': {
            'labels': list(task.keys()),
            'datasets': [{
                'backgroundColor': col,
                'borderColor': bord,
                'borderWidth': 3,
                'data': list(task.values()),
            }]
        },
    })

def plot_video_qc_eid(request, eid):
    extended_qc = Session.objects.get(id=eid).extended_qc
    video = {key: val for key, val in extended_qc.items() if '_video' in key}
    video_data = qc_check.process_video_qc(video)

    return JsonResponse({
        'title': f'Video Left QC: {extended_qc.get("videoLeft", "Not computed")} '
                 f'Video Right QC: {extended_qc.get("videoRight", "Not computed")} '
                 f'Video Body QC: {extended_qc.get("videoBody", "Not computed")}',
        'data': {
            'labels': video_data['label'],
            'datasets': [{
                'backgroundColor': video_data['colour'],
                'data': video_data['data'],
            }]
        },
    })



def plot_video_qc(request, pid):
    extended_qc = ProbeInsertion.objects.get(id=pid).session.extended_qc
    video = {key: val for key, val in extended_qc.items() if '_video' in key}
    video_data = qc_check.process_video_qc(video)

    return JsonResponse({
        'title': f'Video Left QC: {extended_qc.get("videoLeft", "Not computed")} '
                 f'Video Right QC: {extended_qc.get("videoRight", "Not computed")} '
                 f'Video Body QC: {extended_qc.get("videoBody", "Not computed")}',
        'data': {
            'labels': video_data['label'],
            'datasets': [{
                'backgroundColor': video_data['colour'],
                'data': video_data['data'],
            }]
        },
    })


class GalleryTaskView(LoginRequiredMixin, ListView):
    template_name = 'ibl_reports/gallery_task.html'
    login_url = LOGIN_URL

    def get_context_data(self, **kwargs):
        context = super(GalleryTaskView, self).get_context_data(**kwargs)
        session = context['object_list'][0]
        probes = ProbeInsertion.objects.filter(session=session.id)
        dsets = session.data_dataset_session_related

        context['session'] = session.id
        context['data'] = {}
        context['data']['raw_behaviour'] = data_check.raw_behaviour_data_status(dsets, session)
        context['data']['raw_passive'] = data_check.raw_passive_data_status(dsets,session)
        context['data']['raw_ephys'] = data_check.raw_ephys_data_status(dsets, session, probes)
        context['data']['raw_video'] = data_check.raw_video_data_status(dsets, session)
        context['data']['trials'] = data_check.trial_data_status(dsets, session)
        context['data']['wheel'] = data_check.wheel_data_status(dsets, session)
        context['data']['passive'] = data_check.passive_data_status(dsets, session)
        context['data']['ephys'] = data_check.ephys_data_status(dsets, session, probes)
        context['data']['spikesort'] = data_check.spikesort_data_status(dsets, session, probes)
        context['data']['video'] = data_check.video_data_status(dsets, session)
        context['data']['dlc'] = data_check.dlc_data_status(dsets, session)

        return context

    def get_queryset(self):
        eid = self.kwargs.get('eid', None)
        qs = Session.objects.all().filter(id=eid).prefetch_related('data_dataset_session_related')

        return qs


class GallerySessionQcView(LoginRequiredMixin, ListView):
    template_name = 'ibl_reports/gallery_qc.html'
    login_url = LOGIN_URL

    def get_queryset(self):
        eid = self.kwargs.get('eid', None)
        qs = Session.objects.all().filter(id=eid)

        return qs

class GallerySpikeSortingView(LoginRequiredMixin, ListView):
    template_name = 'ibl_reports/gallery_subplots.html'
    login_url = LOGIN_URL

    def get_context_data(self, **kwargs):
        # need to find number of probes
        # need to arrange the photos in the order they expected, if None we just have an empty card
        context = super(GallerySpikeSortingView, self).get_context_data(**kwargs)
        probes = dict()
        for pid in self.pids:
            probes[pid.name] = self.get_spikesorting_plots(context['object_list'].filter(object_id=pid.id))

        context['devices'] = probes
        context['plot_type'] = 'spikesorting'
        return context


    def get_queryset(self):
        eid = self.kwargs.get('eid', None)
        self.pids = ProbeInsertion.objects.all().filter(session=eid)
        qs = Note.objects.all().filter(Q(object_id__in=self.pids.values_list('id', flat=True)))

        return qs

    def get_spikesorting_plots(self, notes):
        plots = ['Cluster Amp vs Depth vs FR',
                 'Firing Rate',
                 'Drift map']
        ordered_plots = {}
        for pl in plots:
            note = notes.filter(text__icontains=pl)
            if note.count() != 0:
                ordered_plots[pl] = note.first()
            else:
                ordered_plots[pl] = None

        return ordered_plots


class GalleryRawEphysView(LoginRequiredMixin, ListView):
    template_name = 'ibl_reports/gallery_subplots.html'
    login_url = LOGIN_URL

    def get_context_data(self, **kwargs):
        # need to find number of probes
        # need to arrange the photos in the order they expected, if None we just have an empty card
        context = super(GalleryRawEphysView, self).get_context_data(**kwargs)
        probes = dict()
        for pid in self.pids:
            probes[pid.name] = self.get_spikesorting_plots(context['object_list'].filter(object_id=pid.id))

        context['devices'] = probes
        context['plot_type'] = 'rawephys'
        return context


    def get_queryset(self):
        eid = self.kwargs.get('eid', None)
        self.pids = ProbeInsertion.objects.all().filter(session=eid)
        qs = Note.objects.all().filter(Q(object_id__in=self.pids.values_list('id', flat=True)))

        return qs

    def get_spikesorting_plots(self, notes):
        plots = ['LFP Spectrum',
                 'rms AP']
        ordered_plots = {}
        for pl in plots:
            note = notes.filter(text__icontains=pl)
            if note.count() != 0:
                ordered_plots[pl] = note.first()
            else:
                ordered_plots[pl] = None

        return ordered_plots


class GalleryHistologyView(LoginRequiredMixin, ListView):
    template_name = 'ibl_reports/gallery_subplots.html'
    login_url = LOGIN_URL

    def get_context_data(self, **kwargs):
        # need to find number of probes
        # need to arrange the photos in the order they expected, if None we just have an empty card
        context = super(GalleryHistologyView, self).get_context_data(**kwargs)
        probes = dict()
        for pid in self.pids:
            probes[pid.name] = self.get_spikesorting_plots(context['object_list'].filter(object_id=pid.id))

        context['devices'] = probes
        context['plot_type'] = 'histology'
        return context


    def get_queryset(self):
        eid = self.kwargs.get('eid', None)
        self.pids = ProbeInsertion.objects.all().filter(session=eid)
        qs = Note.objects.all().filter(Q(object_id__in=self.pids.values_list('id', flat=True)))

        return qs

    def get_spikesorting_plots(self, notes):
        plots = ['CCF']

        ordered_plots = {}
        for pl in plots:
            note = notes.filter(text__icontains=pl)
            if note.count() != 0:
                ordered_plots[pl] = note.first()
            else:
                ordered_plots[pl] = None

        return ordered_plots


class InsertionTableWithFilter(LoginRequiredMixin, ListView):

    login_url = LOGIN_URL
    template_name = 'ibl_reports/table.html'
    paginate_by = 50

    def get_context_data(self, **kwargs):
        context = super(InsertionTableWithFilter, self).get_context_data(**kwargs)
        context['data_status'] = data_check.get_data_status_qs(context['object_list'])
        context['tableFilter'] = self.f

        return context

    def get_queryset(self):

        qs = ProbeInsertion.objects.all().prefetch_related('session__data_dataset_session_related',
                                                           'session', 'session__project',
                                                           'session__subject__lab',)
        # self.f = InsertionFilter(self.request.GET, queryset=qs)
        # qs = self.f.qs
        qs = qs.annotate(task=F('session__extended_qc__task'),
                         video_left=F('session__extended_qc__videoLeft'),
                         video_right=F('session__extended_qc__videoRight'),
                         video_body=F('session__extended_qc__videoBody'),
                         behavior=F('session__extended_qc__behavior'),
                         insertion_qc=F('json__qc'))
        qs = qs.annotate(planned=Exists(
            TrajectoryEstimate.objects.filter(probe_insertion=OuterRef('pk'), provenance=10)))
        qs = qs.annotate(micro=Exists(
            TrajectoryEstimate.objects.filter(probe_insertion=OuterRef('pk'), provenance=30)))
        qs = qs.annotate(histology=Exists(
            TrajectoryEstimate.objects.filter(probe_insertion=OuterRef('pk'), provenance=50,
                                              x__isnull=False)))
        qs = qs.annotate(resolved=F('json__extended_qc__alignment_resolved'))

        self.f = InsertionFilter(self.request.GET, queryset=qs)

        return self.f.qs.order_by('-session__start_time')


class InsertionFilter(django_filters.FilterSet):

    # resolved = django_filters.BooleanFilter(label='resolved', method='filter_resolved')

    class Meta:
        model = ProbeInsertion
        fields = ['id', 'session__lab', 'session__project']
        exclude = ['json']

    def __init__(self, *args, **kwargs):

        super(InsertionFilter, self).__init__(*args, **kwargs)

        self.filters['id'].label = "Probe ID"
        self.filters['session__lab'].label = "Lab"
        self.filters['session__project'].label = "Project"

    def filter_resolved(self, queryset, name, value):
        return queryset.filter(resolved=value)


# def spikesorting_plot(request):
#
#     # brainwide_map
#     raw_ap_files_bwm = Dataset.objects.filter(name__iendswith='.ap.cbin', session__project__name='ibl_neuropixel_brainwide_01')
#     spike_sorted_bwm = raw_ap_files_bwm.filter(probe_insertion__datasets__name='spikes.times.npy')
#     spike_sorted_pyks_bwm = raw_ap_files_bwm.filter(probe_insertion__datasets__collection__icontains='pykilosort')
#
#     # non brainwide map
#     raw_ap_files_pp = Dataset.objects.filter(name__iendswith='.ap.cbin').exclude(
#         session__project__name='ibl_neuropixel_brainwide_01')
#     spike_sorted_pp = raw_ap_files_pp.filter(probe_insertion__datasets__name='spikes.times.npy')
#     spike_sorted_pyks_pp = raw_ap_files_pp.filter(probe_insertion__datasets__collection__icontains='pykilosort')
#
#     return JsonResponse({
#         'title': 'Spike sorting status',
#         'data': {
#             'labels': ['Brainwide map', 'Personal projects'],
#             'datasets': [
#                 {
#                     'label': 'All insertions',
#                     'data': [raw_ap_files_bwm.count(), raw_ap_files_pp.count()],
#                     'backgroundColor': "#FF6384",
#                 },
#                 {
#                     'label': 'Kilosort',
#                     'data': [spike_sorted_bwm.count(), spike_sorted_pp.count()],
#                     'backgroundColor': "#79AEC8",
#                 },
#                 {
#                     'label': 'Pykilosort',
#                     'data': [spike_sorted_pyks_bwm.count(), spike_sorted_pyks_pp.count()],
#                     'backgroundColor': "#417690",
#                 },
#             ]
#         },
#     })


class SpikeSortingTable(LoginRequiredMixin, ListView):
    login_url = LOGIN_URL
    template_name = 'ibl_reports/spikesorting.html'
    paginate_by = 50

    def get_context_data(self, **kwargs):
        context = super(SpikeSortingTable, self).get_context_data(**kwargs)
        context['spikesortingFilter'] = self.f
        context['task'] = data_check.get_spikesorting_task(context['object_list'])

        return context

    def get_queryset(self):

        qs = ProbeInsertion.objects.all().prefetch_related('session__data_dataset_session_related',
                                                           'session', 'session__project',
                                                           'session__subject__lab',)

        qs = qs.annotate(insertion_qc=F('json__qc'))
        qs = qs.annotate(raw=Exists(Dataset.objects.filter(probe_insertion=OuterRef('pk'), name__endswith='ap.cbin')))
        qs = qs.annotate(ks=Exists(Dataset.objects.filter(~Q(collection__icontains='pykilosort'), probe_insertion=OuterRef('pk'),
                                                          name='spikes.times.npy')))
        qs = qs.annotate(pyks=Exists(Dataset.objects.filter(collection__icontains=f'pykilosort', probe_insertion=OuterRef('pk'),
                                                            name='spikes.times.npy')))

        self.f = SpikeSortingFilter(self.request.GET, queryset=qs)

        return self.f.qs.order_by('-session__start_time')


class SpikeSortingFilter(django_filters.FilterSet):
    SPIKESORTINGCHOICES = (
        (0, 'No spikesorting'),
        (1, 'Kilosort'),
        (2, 'Pykilosort'),
    )

    REPEATEDSITE = (
        (0, 'All'),
        (1, 'Repeated Site')
    )

    status = django_filters.ChoiceFilter(choices=SPIKESORTINGCHOICES, method='filter_spikesorting', label='Spike Sorting status')
    repeated = django_filters.ChoiceFilter(choices=REPEATEDSITE, label='Location', method='filter_repeated')

    class Meta:
        model = ProbeInsertion
        fields = ['id', 'session__lab', 'session__project']
        exclude = ['json']

    def __init__(self, *args, **kwargs):

        super(SpikeSortingFilter, self).__init__(*args, **kwargs)

        self.filters['id'].label = "Probe ID"
        self.filters['session__lab'].label = "Lab"
        self.filters['session__project'].label = "Project"

    def filter_spikesorting(self, queryset, name, value):
        if value == '0':
            return queryset.filter(pyks=False, ks=False)
        if value == '1':
            return queryset.filter(pyks=False, ks=True)
        if value == '2':
            return queryset.filter(pyks=True)

    def filter_repeated(self, queryset, name, value):
        if value == '0':
            return queryset
        if value == '1':
            return queryset.filter(Q(trajectory_estimate__provenance=10) & Q(trajectory_estimate__x=-2243) &
                                   Q(trajectory_estimate__y=-2000) &
                                   Q(trajectory_estimate__theta=15) & Q(session__project__name='ibl_neuropixel_brainwide_01'))


class GallerySessionView(LoginRequiredMixin, ListView):
    login_url = LOGIN_URL
    template_name = 'ibl_reports/gallery_session2.html'

    def get_context_data(self, **kwargs):
        context = super(GallerySessionView, self).get_context_data(**kwargs)

        return context

    def get_queryset(self):
        eid = self.kwargs.get('eid', None)
        pids = ProbeInsertion.objects.all().filter(session=eid).values_list('id', flat=True)
        qs = Note.objects.all().filter(Q(object_id=eid) | Q(object_id__in=pids))

        return qs


class GalleryView(LoginRequiredMixin, ListView):
    login_url = LOGIN_URL
    template_name = 'ibl_reports/gallery.html'
    paginate_by = 40

    def get_context_data(self, **kwargs):
        context = super(GalleryView, self).get_context_data(**kwargs)
        context['photoFilter'] = self.f

        return context

    def get_queryset(self):
        # Need a way to do this easily
        qs = Note.objects.all().filter(Q(text__icontains='Single-frame camera') | Q(text__icontains='Drift map'))
        # qs = Note.objects.all().filter(Q(content_type_id=26) | Q(content_type_id=62)).exclude(image='')  # 26 for session, 62 for probe insertion
        qs = qs.annotate(lab=Coalesce(ProbeInsertion.objects.filter(id=OuterRef('object_id')).values('session__lab__name'),
                                      Session.objects.filter(id=OuterRef('object_id')).values('lab__name')))
        qs = qs.annotate(project=Coalesce(ProbeInsertion.objects.filter(id=OuterRef('object_id')).values('session__project__name'),
                                          Session.objects.filter(id=OuterRef('object_id')).values('project__name')))

        self.f = GalleryFilter(self.request.GET, queryset=qs)

        return self.f.qs

plot_options = (
    (0, 'Drift map'),
    (1, 'Single-frame camera'))
class GalleryFilter(django_filters.FilterSet):


    plot = django_filters.ChoiceFilter(choices=plot_options, label='Plot Type', method='filter_plot')
    lab = django_filters.ModelChoiceFilter(queryset=Lab.objects.all(), label='Lab')  # here
    project = django_filters.ModelChoiceFilter(queryset=Project.objects.all(), label='Project', method='filter_project')

    class Meta:
        model = Note
        fields = ['lab', 'project']
        exclude = ['json']

    def __init__(self, *args, **kwargs):
        super(GalleryFilter, self).__init__(*args, **kwargs)

    def filter_project(self, queryset, name, value):

        queryset = queryset.filter(project=value.name)

        return queryset

    def filter_plot(self, queryset, name, value):

        text = [pl[1] for pl in plot_options if pl[0] == int(value)][0]
        queryset = queryset.filter(text__icontains=text)
        return queryset
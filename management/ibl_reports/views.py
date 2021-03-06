import django_filters
from django.http import HttpResponse, JsonResponse
from django.template import loader
from django.views.generic.list import ListView
from django.db.models import Q, F, OuterRef, Exists, UUIDField, DateTimeField, Max, Count
from django.db.models.functions import Coalesce
from django.contrib.auth.mixins import LoginRequiredMixin

from data.models import Dataset
from experiments.models import TrajectoryEstimate, ProbeInsertion
from misc.models import Note, Lab
from subjects.models import Project, Subject
from actions.models import Session

import numpy as np
from ibl_reports import qc_check
from ibl_reports import data_check
from ibl_reports import data_info

LOGIN_URL = '/admin/login/'


def landingpage(request):
    template = loader.get_template('ibl_reports/landing.html')
    context = dict()

    return HttpResponse(template.render(context, request))


# get task qc for plotting
def plot_task_qc_eid(request, eid):
    extended_qc = Session.objects.get(id=eid).extended_qc
    # TODO fix error when the extended qc is None
    task = {key: val for key, val in extended_qc.items() if '_task_' in key}
    col, bord = qc_check.get_task_qc_colours(task)

    task_dict = {}
    task_dict[''] = {'title': f'Task QC: {extended_qc.get("task", "Not computed")}',
                     'data': {
                         'labels': list(task.keys()),
                         'datasets': [{
                             'backgroundColor': col,
                             'borderColor': bord,
                             'borderWidth': 3,
                             'data': list(task.values()),
                         }]
                     },
                     }

    return JsonResponse(task_dict)


# get video qc json for plotting
def plot_video_qc_eid(request, eid):
    extended_qc = Session.objects.get(id=eid).extended_qc
    video_dict = {}
    for cam in ['Body', 'Left', 'Right']:
        video = {key: val for key, val in extended_qc.items() if f'_video{cam}' in key}
        # TODO if null set to 0
        video_data = qc_check.process_video_qc(video)

        video_dict[cam] = {
                        'title': f'Video {cam} QC: {extended_qc.get(f"video{cam}", "Not computed")}',
                        'data': {
                            'labels': video_data['label'],
                            'datasets': [{
                                'backgroundColor': video_data['colour'],
                                'data': video_data['data'],
                            }]
                        },
        }

    return JsonResponse(video_dict)


# get dlc qc json for plotting
def plot_dlc_qc_eid(request, eid):
    extended_qc = Session.objects.get(id=eid).extended_qc
    dlc_dict = {}
    for cam in ['Body', 'Left', 'Right']:
        dlc = {key: val for key, val in extended_qc.items() if f'_dlc{cam}' in key}
        # TODO if null set to 0
        dlc_data = qc_check.process_video_qc(dlc)

        dlc_dict[cam] = {
                        'title': f'DLC {cam} QC: {extended_qc.get(f"dlc{cam}", "Not computed")}',
                        'data': {
                            'labels': dlc_data['label'],
                            'datasets': [{
                                'backgroundColor': dlc_data['colour'],
                                'data': dlc_data['data'],
                            }]
                        },
        }

    return JsonResponse(dlc_dict)


# Insertion overview page
class InsertionOverview(LoginRequiredMixin, ListView):
    template_name = 'ibl_reports/plots.html'
    login_url = LOGIN_URL

    def get_context_data(self, **kwargs):
        context = super(InsertionOverview, self).get_context_data(**kwargs)
        probe = context['object_list'][0]
        session = Session.objects.get(id=probe.session.id)
        dsets = session.data_dataset_session_related

        context['probe'] = probe
        context['session'] = session
        context['data'] = {}
        context['data']['raw_behaviour'] = data_check.raw_behaviour_data_status(dsets, session)
        context['data']['raw_passive'] = data_check.raw_passive_data_status(dsets, session)
        context['data']['raw_ephys'] = data_check.raw_ephys_data_status(dsets, session, [probe])
        context['data']['raw_video'] = data_check.raw_video_data_status(dsets, session)
        context['data']['trials'] = data_check.trial_data_status(dsets, session)
        context['data']['wheel'] = data_check.wheel_data_status(dsets, session)
        context['data']['passive'] = data_check.passive_data_status(dsets, session)
        context['data']['ephys'] = data_check.ephys_data_status(dsets, session, [probe])
        context['data']['spikesort'] = data_check.spikesort_data_status(dsets, session, [probe])
        context['data']['video'] = data_check.video_data_status(dsets, session)
        context['data']['dlc'] = data_check.dlc_data_status(dsets, session)

        return context

    def get_queryset(self):
        self.pid = self.kwargs.get('pid', None)
        qs = ProbeInsertion.objects.all().filter(id=self.pid)

        return qs


# Insertion table page
class InsertionTable(LoginRequiredMixin, ListView):

    login_url = LOGIN_URL
    template_name = 'ibl_reports/table.html'
    paginate_by = 50

    def get_context_data(self, **kwargs):
        context = super(InsertionTable, self).get_context_data(**kwargs)
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

    id = django_filters.CharFilter(label='Experiment ID/ Probe ID', method='filter_id', lookup_expr='startswith')

    class Meta:
        model = ProbeInsertion
        fields = ['session__lab', 'session__project']
        exclude = ['json']

    def __init__(self, *args, **kwargs):

        super(InsertionFilter, self).__init__(*args, **kwargs)

        self.filters['session__lab'].label = "Lab"
        self.filters['session__project'].label = "Project"

    def filter_id(self, queryset, name, value):
        queryset = queryset.filter(Q(session__id__startswith=value) | Q(id__startswith=value))
        return queryset


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

    id = django_filters.CharFilter(label='Experiment ID/ Probe ID', method='filter_id', lookup_expr='startswith')
    status = django_filters.ChoiceFilter(choices=SPIKESORTINGCHOICES, method='filter_spikesorting', label='Spike Sorting status')
    repeated = django_filters.ChoiceFilter(choices=REPEATEDSITE, label='Location', method='filter_repeated')

    class Meta:
        model = ProbeInsertion
        fields = ['session__lab', 'session__project']
        exclude = ['json']

    def __init__(self, *args, **kwargs):

        super(SpikeSortingFilter, self).__init__(*args, **kwargs)

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

    def filter_id(self, queryset, name, value):
        queryset = queryset.filter(Q(session__id__startswith=value) | Q(id__startswith=value))
        return queryset


class GalleryTaskView(LoginRequiredMixin, ListView):
    template_name = 'ibl_reports/gallery_task.html'
    login_url = LOGIN_URL

    def get_context_data(self, **kwargs):
        context = super(GalleryTaskView, self).get_context_data(**kwargs)
        session = context['object_list'][0]
        probes = ProbeInsertion.objects.filter(session=session.id).prefetch_related('datasets')
        dsets = session.data_dataset_session_related.select_related('dataset_type')

        context['session'] = session
        context['data'] = {}
        context['data']['raw_behaviour'] = data_check.raw_behaviour_data_status(dsets, session)
        context['data']['raw_passive'] = data_check.raw_passive_data_status(dsets, session)
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
        qs = Session.objects.filter(id=eid).prefetch_related('data_dataset_session_related', 'tasks')

        return qs


class GallerySessionQcView(LoginRequiredMixin, ListView):
    template_name = 'ibl_reports/gallery_qc.html'
    login_url = LOGIN_URL

    def get_context_data(self, **kwargs):
        context = super(GallerySessionQcView, self).get_context_data(**kwargs)
        context['session'] = context['object_list'][0]

        return context

    def get_queryset(self):
        self.eid = self.kwargs.get('eid', None)
        qs = Session.objects.all().filter(id=self.eid)

        return qs


class GallerySubPlotProbeView(LoginRequiredMixin, ListView):
    template_name = 'ibl_reports/gallery_subplots.html'
    login_url = LOGIN_URL
    plot_type = None

    def get_context_data(self, **kwargs):
        # need to find number of probes
        # need to arrange the photos in the order they expected, if None we just have an empty card
        context = super(GallerySubPlotProbeView, self).get_context_data(**kwargs)

        probes = dict()
        for pid in self.pids:
            probes[pid.name] = data_check.get_plots(context['object_list'].filter(object_id=pid.id), self.plot_type)

        context['session'] = Session.objects.all().get(id=self.eid)
        context['devices'] = probes
        context['plot_type'] = self.plot_type
        return context

    def get_queryset(self):
        self.eid = self.kwargs.get('eid', None)
        self.pids = ProbeInsertion.objects.all().filter(session=self.eid).order_by('name')
        qs = Note.objects.all().filter(Q(object_id__in=self.pids.values_list('id', flat=True)))

        return qs


class GallerySubPlotSessionView(LoginRequiredMixin, ListView):
    template_name = 'ibl_reports/gallery_subplots.html'
    login_url = LOGIN_URL
    plot_type = None

    def get_context_data(self, **kwargs):
        # need to find number of probes
        # need to arrange the photos in the order they expected, if None we just have an empty card
        context = super(GallerySubPlotSessionView, self).get_context_data(**kwargs)
        probes = {}
        probes[''] = data_check.get_plots(context['object_list'], self.plot_type)

        context['session'] = Session.objects.all().get(id=self.eid)
        context['devices'] = probes
        context['plot_type'] = self.plot_type
        return context

    def get_queryset(self):
        self.eid = self.kwargs.get('eid', None)
        qs = Note.objects.all().filter(object_id=self.eid)

        return qs


class GallerySessionView(LoginRequiredMixin, ListView):
    login_url = LOGIN_URL
    template_name = 'ibl_reports/gallery_session.html'

    def get_context_data(self, **kwargs):
        context = super(GallerySessionView, self).get_context_data(**kwargs)
        context['session'] = Session.objects.all().get(id=self.eid)

        return context

    def get_queryset(self):
        self.eid = self.kwargs.get('eid', None)
        pids = ProbeInsertion.objects.all().filter(session=self.eid).values_list('id', flat=True)
        qs = Note.objects.all().filter(Q(object_id=self.eid) | Q(object_id__in=pids))
        qs = qs.filter(json__tag="## report ##")

        return qs


class GalleryOverviewView(LoginRequiredMixin, ListView):
    login_url = LOGIN_URL
    template_name = 'ibl_reports/gallery_overview.html'

    def get_context_data(self, **kwargs):
        context = super(GalleryOverviewView, self).get_context_data(**kwargs)
        context['session'] = Session.objects.all().get(id=self.eid)

        context['behaviour'] = qc_check.behav_summary(context['session'].extended_qc)
        context['qc'] = qc_check.qc_summary(context['session'].extended_qc)

        probes = ProbeInsertion.objects.all().filter(session=self.eid).prefetch_related('trajectory_estimate').order_by('name')
        probes = probes.annotate(planned=Exists(
            TrajectoryEstimate.objects.filter(probe_insertion=OuterRef('pk'), provenance=10)))
        probes = probes.annotate(micro=Exists(
            TrajectoryEstimate.objects.filter(probe_insertion=OuterRef('pk'), provenance=30)))
        probes = probes.annotate(histology=Exists(
            TrajectoryEstimate.objects.filter(probe_insertion=OuterRef('pk'), provenance=50,
                                              x__isnull=False)))
        probes = probes.annotate(aligned=Exists(
            TrajectoryEstimate.objects.filter(probe_insertion=OuterRef('pk'), provenance=70)))
        probes = probes.annotate(resolved=F('json__extended_qc__alignment_resolved'))
        probes = probes.annotate(qc=F('json__qc'))
        probes = probes.annotate(n_units=F('json__n_units'))
        probes = probes.annotate(n_good_units=F('json__n_units_qc_pass'))
        context['probes'] = probes

        return context

    def get_queryset(self):
        self.eid = self.kwargs.get('eid', None)
        qs = Session.objects.filter(id=self.eid)
        return qs


def add_plot_qc(request):
    pid = request.POST.get('pid')
    value = request.POST.get('value')

    probe = ProbeInsertion.objects.get(id=pid)
    plot_qc = probe.json.get('extended_qc', {}).get('experimenter_raw_destripe', None)
    if plot_qc != 'pass':
        probe.json['extended_qc'].update(experimenter_raw_destripe=value)

    probe.save()

    return JsonResponse({'Success': 'json field updates'})


class GalleryPlotsOverview(LoginRequiredMixin, ListView):
    login_url = LOGIN_URL
    template_name = 'ibl_reports/gallery_plots_overview.html'
    paginate_by = 40

    def get_context_data(self, **kwargs):
        context = super(GalleryPlotsOverview, self).get_context_data(**kwargs)
        context['photoFilter'] = self.f

        return context

    def get_queryset(self):

        qs = Note.objects.all().filter(json__tag="## report ##")
        qs = qs.annotate(lab=Coalesce(ProbeInsertion.objects.filter(id=OuterRef('object_id')).values('session__lab__name'),
                                      Session.objects.filter(id=OuterRef('object_id')).values('lab__name')))
        qs = qs.annotate(project=Coalesce(ProbeInsertion.objects.filter(id=OuterRef('object_id')).values('session__project__name'),
                                          Session.objects.filter(id=OuterRef('object_id')).values('project__name')))
        qs = qs.annotate(session=Coalesce(ProbeInsertion.objects.filter(id=OuterRef('object_id')).values('session'),
                                          Session.objects.filter(id=OuterRef('object_id')).values('pk'), output_field=UUIDField()))

        qs = qs.annotate(session_time=Coalesce(ProbeInsertion.objects.filter(id=OuterRef('object_id')).values('session__start_time'),
                                               Session.objects.filter(id=OuterRef('object_id')).values('start_time')))

        qs = qs.annotate(session_qc=Coalesce(ProbeInsertion.objects.filter(id=OuterRef('object_id')).values('session__qc'),
                                             Session.objects.filter(id=OuterRef('object_id')).values('qc')))

        qs = qs.annotate(behav_qc=Coalesce(ProbeInsertion.objects.filter(id=OuterRef('object_id')).values('session__extended_qc__behavior'),
                                             Session.objects.filter(id=OuterRef('object_id')).values('extended_qc__behavior')))

        qs = qs.annotate(probe_qc=ProbeInsertion.objects.filter(id=OuterRef('object_id')).values('json__qc'))

        qs = qs.annotate(destripe_qc=ProbeInsertion.objects.filter(
            id=OuterRef('object_id')).values('json__extended_qc__experimenter_raw_destripe'))

        qs = qs.annotate(plot_type=Note.objects.filter(id=OuterRef('id')).values('json__name'))

        self.f = GalleryFilter(self.request.GET, queryset=qs.order_by('-session_time'))

        return self.f.qs


plot_types = Note.objects.all().filter(json__tag="## report ##").values_list("text", flat=True).distinct()
PLOT_OPTIONS = []
for ip, pl in enumerate(plot_types):
    PLOT_OPTIONS.append((ip, pl))


class GalleryFilter(django_filters.FilterSet):
    """
    Class that filters over Notes queryset.
    Annotations are provided by the list view
    """
    REPEATEDSITE = (
        (0, 'All'),
        (1, 'Repeated Site')
    )

    DESTRIPE_QC = (
        (0, 'Not Set'),
        (1, 'Check'),
        (2, 'Pass')
    )

    CRITICAL_QC = (
        (0, 'Non-critical'),
        (1, 'Critical'),
    )

    BEHAVIOR_QC = (
        (0, 'Pass'),
        (1, 'Fail'),
    )


    id = django_filters.CharFilter(label='Experiment ID/ Probe ID', method='filter_id', lookup_expr='startswith')
    plot = django_filters.ChoiceFilter(choices=PLOT_OPTIONS, label='Plot Type', method='filter_plot')
    lab = django_filters.ModelChoiceFilter(queryset=Lab.objects.all(), label='Lab')
    project = django_filters.ModelChoiceFilter(queryset=Project.objects.all(), label='Project', method='filter_project')
    repeated = django_filters.ChoiceFilter(choices=REPEATEDSITE, label='Location', method='filter_repeated')
    critical_qc = django_filters.ChoiceFilter(choices=CRITICAL_QC, label='QC', method='filter_critical_qc')
    behavior_qc = django_filters.ChoiceFilter(choices=BEHAVIOR_QC, label='Behaviour', method='filter_behav_qc')
    destripe_qc = django_filters.ChoiceFilter(choices=DESTRIPE_QC, label='Destripe QC', method='filter_destripe_qc')

    class Meta:
        model = Note
        fields = ['lab', 'project']
        exclude = ['json']

    def __init__(self, *args, **kwargs):
        super(GalleryFilter, self).__init__(*args, **kwargs)

    def filter_behav_qc(self, queryset, name, value):
        if value == '0':
            queryset = queryset.filter(behav_qc=1)
        elif value == '1':
            queryset = queryset.filter(behav_qc=0)

        return queryset


    def filter_critical_qc(self, queryset, name, value):

        if value == '0':
            queryset = queryset.exclude(Q(probe_qc='CRITICAL') | Q(session_qc=50))
        elif value == '1':
            queryset = queryset.filter(Q(probe_qc='CRITICAL') | Q(session_qc=50))

        return queryset

    def filter_destripe_qc(self, queryset, name, value):

        if value == '0':
            queryset = queryset.filter(destripe_qc__isnull=True)
        elif value == '1':
            queryset = queryset.filter(destripe_qc='check')
        elif value == '2':
            queryset = queryset.filter(destripe_qc='pass')

        return queryset

    def filter_project(self, queryset, name, value):
        queryset = queryset.filter(project=value.name)
        return queryset

    def filter_plot(self, queryset, name, value):
        text = [pl[1] for pl in PLOT_OPTIONS if pl[0] == int(value)][0]
        queryset = queryset.filter(text=text)
        return queryset

    def filter_id(self, queryset, name, value):
        queryset = queryset.filter(Q(object_id__startswith=value) | Q(session__startswith=value))
        return queryset

    def filter_repeated(self, queryset, name, value):
        pids = ProbeInsertion.objects.filter(trajectory_estimate__provenance=10,
                                             trajectory_estimate__x=-2243,
                                             trajectory_estimate__y=-2000,
                                             trajectory_estimate__theta=15)
        if value == '0':
            return queryset
        if value == '1':
            return queryset.filter(Q(object_id__in=pids.values_list('id')) | Q(object_id__in=pids.values_list('session')))


class SessionImportantPlots(LoginRequiredMixin, ListView):
    template_name = 'ibl_reports/gallery_session_overview.html'
    login_url = LOGIN_URL
    paginate_by = 20

    def get_context_data(self, **kwargs):
        # need to figure out which is more efficient
        context = super(SessionImportantPlots, self).get_context_data(**kwargs)
        context['sessionFilter'] = self.f
        notes = Note.objects.all().filter(json__tag="## report ##")
        s, data = self.get_my_data(context['object_list'], notes)

        context['info'] = data
        context['sessions'] = s

        return context

    def get_my_data(self, sessions, notes):
        data = []
        s = []
        for sess in sessions:
            info = {}
            s.append(sess)
            plot_dict = {}
            for plot in data_info.OVERVIEW_SESSION_PLOTS:
                note = notes.filter(object_id=sess.id, text=plot[0]).first()
                if not note and not plot[1]:
                    continue
                else:
                    plot_dict[plot[0]] = note
            info['session'] = plot_dict
            probes = sess.probe_insertion.values().order_by('name')
            if probes.count() > 0:
                for probe in probes:
                    plot_dict = {}
                    for plot in data_info.OVERVIEW_PROBE_PLOTS:
                        note = notes.filter(object_id=probe['id'], text=plot[0]).first()
                        if not note and not plot[1]:
                            continue
                        else:
                            plot_dict[plot[0]] = note
                    info[f'{probe["name"]}  {probe["id"]}'] = plot_dict
            data.append(info)

        return s, data

    def get_queryset(self):
        qs = Session.objects.filter(task_protocol__icontains='ephysChoiceWorld',
                                    json__IS_MOCK=False).prefetch_related('probe_insertion')
        self.f = SessionFilter(self.request.GET, queryset=qs)

        return self.f.qs.order_by('-start_time')


class SessionFilter(django_filters.FilterSet):

    REPEATEDSITE = (
        (0, 'All'),
        (1, 'Repeated Site')
    )

    id = django_filters.CharFilter(label='Experiment ID/ Probe ID', method='filter_id', lookup_expr='startswith')
    lab = django_filters.ModelChoiceFilter(queryset=Lab.objects.all(), label='Lab')
    project = django_filters.ModelChoiceFilter(queryset=Project.objects.all(), label='Project')
    repeated = django_filters.ChoiceFilter(choices=REPEATEDSITE, label='Location', method='filter_repeated')

    class Meta:
        model = Note
        fields = ['lab', 'project']
        exclude = ['json']

    def __init__(self, *args, **kwargs):
        super(SessionFilter, self).__init__(*args, **kwargs)

    def filter_id(self, queryset, name, value):
        queryset = queryset.filter(Q(id__startswith=value) | Q(probe_insertion__id__startswith=value)).distinct()
        return queryset

    def filter_repeated(self, queryset, name, value):
        if value == '0':
            return queryset
        if value == '1':
            return queryset.filter(Q(probe_insertion__trajectory_estimate__provenance=10) &
                                   Q(probe_insertion__trajectory_estimate__x=-2243) &
                                   Q(probe_insertion__trajectory_estimate__y=-2000) &
                                   Q(probe_insertion__trajectory_estimate__theta=15))


class SubjectTrainingPlots(LoginRequiredMixin, ListView):
    template_name = 'ibl_reports/gallery_subject_overview.html'
    login_url = LOGIN_URL
    paginate_by = 20

    def get_context_data(self, **kwargs):
        # need to figure out which is more efficient
        context = super(SubjectTrainingPlots, self).get_context_data(**kwargs)
        context['subjectFilter'] = self.f
        notes = Note.objects.all().filter(json__tag="## report ##")
        s, data = self.get_my_data(context['object_list'], notes)

        context['info'] = data
        context['subjects'] = s

        return context

    def get_my_data(self, subjects, notes):
        data = []
        s = []
        for subj in subjects:
            info = {}
            s.append(subj)
            plot_dict = {}
            for plot in data_info.OVERVIEW_SUBJECT_PLOTS:
                note = notes.filter(object_id=subj.id, text=plot[0]).first()
                if not note and not plot[1]:
                    continue
                else:
                    plot_dict[plot[0]] = note
            info[''] = plot_dict
            data.append(info)

        return s, data

    def get_queryset(self):
        qs = Subject.objects.all().prefetch_related('actions_sessions')
        qs = qs.annotate(latest_sess=Max('actions_sessions__start_time'), n_sess=Count('actions_sessions'))
        qs = qs.filter(n_sess__gte=1)
        qs = qs.order_by('-latest_sess')
        self.f = SubjectFilter(self.request.GET, queryset=qs)

        return self.f.qs


class SubjectFilter(django_filters.FilterSet):

    nickname = django_filters.ModelChoiceFilter(queryset=Subject.objects.all(), label='Nickname')
    lab = django_filters.ModelChoiceFilter(queryset=Lab.objects.all(), label='Lab')

    class Meta:
        model = Note
        fields = ['nickname', 'lab']
        exclude = ['json']

    def __init__(self, *args, **kwargs):
        super(SubjectFilter, self).__init__(*args, **kwargs)

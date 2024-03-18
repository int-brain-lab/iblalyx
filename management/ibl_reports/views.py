from datetime import date
import logging
import time
import io

import django_filters
from django.http import HttpResponse, JsonResponse
from django.template import loader
from django.views.generic.list import ListView
from django.db.models import Q, F, OuterRef, Exists, UUIDField, Max, Count, Func, Subquery, TextField
from django.db.models.functions import Coalesce, Cast
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.postgres.fields import JSONField, ArrayField
from django.core.files.storage import default_storage

from experiments.models import TrajectoryEstimate, ProbeInsertion
from misc.models import Note, Lab
from subjects.models import Project, Subject
from actions.models import Session
from jobs.models import Task

import pyarrow.parquet as pq
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import to_hex

from ibl_reports import qc_check
from ibl_reports import data_check
from ibl_reports import data_info

LOGIN_URL = '/admin/login/'

logger = logging.getLogger(__name__)


def landingpage(request):
    template = loader.get_template('ibl_reports/landing.html')
    context = dict()
    return HttpResponse(template.render(context, request))


class PairedRecordingsView(LoginRequiredMixin, ListView):
    template_name = 'ibl_reports/paired_recordings.html'
    login_url = LOGIN_URL

    @property
    def df_paired_experiments(self):
        logger.info(f'Getting paired experiments files from the media storage backend {default_storage}')
        with default_storage.open('paired_experiments.pqt') as fp:
            df = pq.read_table(fp).to_pandas()
        logger.info('Download successful')
        return df

    def get_context_data(self, **kwargs):
        from iblatlas.regions import BrainRegions
        from iblutil.numerical import ismember
        import scipy.sparse as sp

        regions = BrainRegions()  # todo need to cache this
        context = super(PairedRecordingsView, self).get_context_data(**kwargs)
        context['pairedFilter'] = self.f
        sessions = context['object_list']
        sessions = sessions.annotate(eid=Cast('id', output_field=TextField()))
        eids = sessions.values_list('eid', flat=True)
        paired_experiments = self.df_paired_experiments
        mapping_choice = self.request.GET.get('mapping', '1')
        paired_experiments = paired_experiments[paired_experiments['eid'].isin(eids)]
        if mapping_choice == '0':
            mapping = ['VISp', 'VISam', 'MOs', 'PL', 'LP', 'VAL', 'CP', 'GPe', 'SNr', 'SCm', 'MRN', 'ZI',
                       'FN', 'IRN', 'GRN', 'PRNr']
        else:
            mapping = 'Cosmos'

        if isinstance(mapping, str):  # in this case we compute the paired recordings for a specific mapping
            mapped_ids = np.unique(regions.id[regions.mappings[mapping]])
        else:  # in this case we compute the mapping corresponding to a list of custom regions, taking into
            # account the hierarchical nature of the brain atlas
            acronyms = mapping
            mapped_ids = np.unique(np.r_[regions.acronym2id(acronyms), 0])
            mapping = np.zeros_like(regions.id)
            for aid in regions.acronym2id(acronyms):
                descendants = regions.descendants(aid)['id']
                irs, _ = ismember(np.abs(regions.id), descendants)  # NB: this is where to work on multi hemisphere
                mapping[irs] = np.where(aid == regions.id)[0][0]

        # remaps the regions to the target map
        paired_experiments['aida'] = regions.remap(paired_experiments['aida'], source_map='Allen', target_map=mapping)
        paired_experiments['aidb'] = regions.remap(paired_experiments['aidb'], source_map='Allen', target_map=mapping)

        # aggregate per per of regions
        plinks = paired_experiments.groupby(['aida', 'aidb']).aggregate(
            n_experiments=pd.NamedAgg(column='eid', aggfunc='nunique')).reset_index()
        # compute the paired recording matrix indices
        _, plinks['ia'] = ismember(plinks['aida'], mapped_ids)
        _, plinks['ib'] = ismember(plinks['aidb'], mapped_ids)
        values = np.r_[plinks['n_experiments'], plinks['n_experiments']] / 2
        ia = np.r_[plinks['ia'], plinks['ib']]
        ib = np.r_[plinks['ib'], plinks['ia']]
        # compute the matrix from the indices and values
        shared_recordings = sp.coo_matrix((values, (ia, ib)), shape=(mapped_ids.size, mapped_ids.size)).todense()
        shared_recordings = shared_recordings[1:, 1:]

        def rgb_to_hex(r, g, b):
            return '#{:02x}{:02x}{:02x}'.format(r, g, b)

        nodes = []
        for r in regions.id2acronym(mapped_ids[1:]):
            nodes.append({'name': r, 'color': rgb_to_hex(*regions.rgb[regions.acronym2index(r)[1][0][0]])})

        links = []
        for i in range(shared_recordings.shape[0]):
            for j in range(shared_recordings.shape[1]):
                links.append({'source': i, 'target': j, 'value': shared_recordings[i, j]})

        context['nodes'] = nodes
        context['links'] = links
        context['data'] = shared_recordings

        return context

    def get_queryset(self):
        # Not optimal as we load this twice....
        paired_experiments = self.df_paired_experiments
        eids = paired_experiments.eid.unique()
        qs = Session.objects.filter(id__in=eids)
        self.f = PairedFilter(self.request.GET, queryset=qs)
        return self.f.qs


class SubqueryArray(Subquery):
    template = 'ARRAY(%(subquery)s)'
    @property
    def output_field(self):
        output_fields = [x.output_field for x in self.get_source_expressions()]
        return ArrayField(base_field=output_fields[0])


class PairedFilter(django_filters.FilterSet):
    """
    Class that filters over Notes queryset.
    Annotations are provided by the list view
    """
    MAPPING = (
        (0, 'U19'),
        (1, 'Cosmos')
    )

    PROVENANCE = (
        (0, 'All'),
        (1, 'Histology'),
        (2, 'Resolved')
    )

    CRITICAL_QC = (
        (0, 'All'),
        (1, 'Non-critical'),
    )

    mapping = django_filters.ChoiceFilter(choices=MAPPING, label='Mapping', method='filter_mapping')
    project = django_filters.ModelChoiceFilter(queryset=Project.objects.all(), label='Project', method='filter_project')
    critical_qc = django_filters.ChoiceFilter(choices=CRITICAL_QC, label='QC', method='filter_critical_qc')
    provenance_qc = django_filters.ChoiceFilter(choices=PROVENANCE, label='Provenance', method='filter_provenance')

    class Meta:
        model = Session
        fields = ['project']
        exclude = ['json']

    def __init__(self, *args, **kwargs):
        super(PairedFilter, self).__init__(*args, **kwargs)

    def filter_critical_qc(self, queryset, name, value):

        if value == '0':
            qs = queryset
        else:
            # Filter for critical sessions
            qs = queryset.exclude(extended_qc__icontains='critical')
            # Annotate with critical insertions and remove associated sessions
            filt = ProbeInsertion.objects.filter(session=OuterRef('id')).values('json__qc')
            qs = qs.annotate(probe_qc=SubqueryArray(filt)).exclude(probe_qc__icontains='critical')

        return qs

    def filter_project(self, queryset, name, value):
        queryset = queryset.filter(project__name=value.name)
        return queryset

    def filter_mapping(self, queryset, name, value):
        return queryset

    def filter_provenance(self, queryset, name, value):

        class JsonBuildObject(Func):
            function = 'to_jsonb'
            output_field = JSONField()

        if value == '0':
            qs = queryset
        elif value == '1':
            filt = ProbeInsertion.objects.filter(session=OuterRef('id')).annotate(
                hist=Coalesce(F('json__extended_qc__tracing_exists'),
                              JsonBuildObject(False), output_field=JSONField())).values('hist')
            qs = queryset.annotate(histology=SubqueryArray(filt))
            qs = qs.filter(histology__icontains=True).exclude(histology__icontains=False)
        else:
            filt = ProbeInsertion.objects.filter(session=OuterRef('id')).annotate(
                aligned=Coalesce(F('json__extended_qc__alignment_resolved'),
                                 JsonBuildObject(False), output_field=JSONField())).values('aligned')
            qs = queryset.annotate(alignment=SubqueryArray(filt))
            qs = qs.filter(alignment__icontains=True).exclude(alignment__icontains=False)

        return qs


# get task qc for plotting
def plot_task_qc_eid(request, eid):
    extended_qc = Session.objects.get(id=eid).extended_qc
    # TODO fix error when the extended qc is None
    task = {key: val for key, val in extended_qc.items() if '_task_' in key}
    col, bord, thres, outcome, labels, vals = qc_check.get_task_qc_colours(task)

    task_dict = {}
    task_dict[''] = {'title': f'Task QC: {extended_qc.get("task", "Not computed")}',
                     'thresholds': thres,
                     'outcomes': outcome,
                     'data': {
                         'labels': list(labels),
                         'datasets': [{
                             'backgroundColor': col,
                             'borderColor': bord,
                             'borderWidth': 3,
                             'data': vals,
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
        video_data, outcome = qc_check.process_video_qc(video)

        video_dict[cam] = {
                        'title': f'Video {cam} QC: {extended_qc.get(f"video{cam}", "Not computed")}',
                        'thresholds': [],
                        'outcomes': outcome,
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
        dlc_data, outcome = qc_check.process_video_qc(dlc)

        dlc_dict[cam] = {
                        'title': f'DLC {cam} QC: {extended_qc.get(f"dlc{cam}", "Not computed")}',
                        'thresholds': [],
                        'outcomes': outcome,
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


class GalleryTaskView(LoginRequiredMixin, ListView):
    template_name = 'ibl_reports/gallery_task.html'
    login_url = LOGIN_URL

    def get_context_data(self, **kwargs):
        context = super(GalleryTaskView, self).get_context_data(**kwargs)
        session = context['object_list'][0]
        context['session'] = session
        context['tasks'] = Task.objects.filter(session=session)

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
        context['session'] = Session.objects.get(id=self.eid)

        context['behaviour'] = qc_check.behav_summary(context['session'].extended_qc)
        context['qc'] = qc_check.qc_summary(context['session'].extended_qc)

        probes = ProbeInsertion.objects.filter(session=self.eid).prefetch_related('trajectory_estimate').order_by('name')
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
        qs = qs.annotate(project=Coalesce(ProbeInsertion.objects.filter(id=OuterRef('object_id')).values('session__projects__name'),
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
    paginate_by = 15

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
            qc_info = {}

            qc_info['Tasks'] = len(sess.tasks.all())
            qc_info['Dsets'] = len(sess.data_dataset_session_related.all())
            qc_info['session'] = sess.get_qc_display

            if sess.extended_qc:
                for key, val in sess.extended_qc.items():
                    if key[0] != '_':
                        if key == 'behavior':
                            val = 'PASS' if 1 else 'FAIL'
                        qc_info[key] = val

            sess.qc_info = qc_info
            s.append(sess)

            plot_dict = {}
            for plot in data_info.OVERVIEW_SESSION_PLOTS:
                note = notes.filter(object_id=sess.id, text=plot[0]).first()
                if note:
                    plot_dict[plot[0]] = note

            probes = sess.probe_insertion.values().order_by('name')
            if probes.count() > 0:
                for probe in probes:
                    for plot in data_info.OVERVIEW_PROBE_PLOTS:
                        note = notes.filter(object_id=probe['id'], text=plot[0]).first()
                        if note:
                            plot_dict[f'{plot[0]}_{probe["name"]}'] = note

            data.append(plot_dict)

        return s, data

    def get_queryset(self):
        qs = Session.objects.all().prefetch_related('probe_insertion')

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
    statuses = (
        'habituation', 'in_training', 'trained_1a', 'trained_1b',
        'ready4ephysrig', 'ready4delay', 'ready4recording', 'untrainable', 'unbiasable')

    def get_context_data(self, **kwargs):
        # need to figure out which is more efficient
        context = super(SubjectTrainingPlots, self).get_context_data(**kwargs)
        context['subjectFilter'] = self.f
        notes = Note.objects.all().filter(json__tag="## report ##")
        s, data = self.get_my_data(context['object_list'], notes)

        context['info'] = data
        context['subjects'] = s
        if not self.request.GET.get('nickname'):
            context['status_data'] = self.get_all_subjects_status_data(lab=self.request.GET.get('lab'))
        return context

    def get_my_data(self, subjects, notes):
        data = []
        s = []
        for subj in subjects:
            info = {}
            s.append(subj)
            plot_dict = {}
            for plot in data_info.OVERVIEW_SUBJECT_PLOTS:
                note = notes.filter(object_id=subj.id, text=plot[0]).order_by('-date_time').first()
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

    def get_all_subjects_status_data(self, lab=None):
        """
        Fetch the data required for the subjects training status plot.

        A plot of subject (y-axis) vs date (x-axis), which each point a session whose colour
        corresponds to the training status on that session.

        :param lab: The UUID of the lab whose subjects are plotted. If None, all active subjects are plotted.
        """
        # We handle only the live mice that are in the training pipeline
        filter_args = dict(cull__isnull=True, death_date__isnull=True, json__has_key='trained_criteria')
        if lab:
            filter_args['lab'] = lab
        subjects = (Subject
                    .objects
                    .filter(**filter_args)
                    .extra(where=['''
                        subjects_subject.id IN
                        (SELECT subject_id FROM actions_waterrestriction
                        WHERE end_time IS NULL)
                        ''']))
        if subjects.count() == 0:
            return {}
        subject_data = subjects.values_list('nickname', 'json')
        names, crit = zip(*[(name, jsn.get('trained_criteria', {})) for name, jsn in subject_data])
        training_status = pd.DataFrame.from_records(crit, index=names)
        # Drop eids and parse dates
        training_status = (training_status[~training_status.isna()]
                           .applymap(lambda x: date.fromisoformat(x[0]), na_action='ignore'))
        sessions = (Session
                    .objects
                    .select_related('subject')
                    .filter(subject__in=subjects, procedures__name='Behavior training/tasks')
                    .values_list('subject__nickname', 'pk', 'start_time__date'))
        sessions = (pd.DataFrame
                    .from_records(sessions, columns=('subject', 'eid', 'date'))
                    .set_index('subject'))

        # Create map of training status -> mice
        mice_by_status = (training_status
                          .applymap(lambda d: time.mktime(d.timetuple()), na_action='ignore')
                          .idxmax(axis=1, skipna=True)  # status for latest date
                          .to_frame()  # allows us to call groupby on values without assignment
                          .groupby(0))
        # Sort dict by status
        mice_by_status = sorted(mice_by_status, key=lambda item: self.statuses.index(item[0]))
        all_subjects = training_status.index.tolist()
        all_data = {'datasets': [],
                    'subject_map': {i: s for i, s in enumerate(all_subjects)},
                    'mice_by_status': {x: y.index.tolist() for x, y in mice_by_status}}
        colour_map = self.status_colour_map()
        for i, status in enumerate(filter(lambda s: s in training_status.columns, self.statuses)):
            data = {'label': status, 'backgroundColor': colour_map[status], 'data': []}
            # Get a map of subject name and date on which status reached
            for subject, date_reached in training_status[status].items():
                subject_idx = all_subjects.index(subject)
                if not isinstance(date_reached, date):
                    continue  # Status not met for this subject; nothing to plot
                # Subject map of status -> date reached
                status_dates = training_status.loc[subject]
                # In some cases, no sessions as they haven't been annotated as behavior/training task
                try:
                    # Unique dates of all this subject's sessions
                    session_dates = np.unique(sessions.loc[subject, 'date'])  # use np because sometimes returns single value
                except KeyError:
                    continue
                # Plot session dates on or after date when status reached, up until the next of the next status
                session_dates = session_dates[session_dates >= date_reached]
                next_date = status_dates[status_dates > date_reached].sort_values()
                if not next_date.empty:
                    session_dates = session_dates[session_dates <= next_date[0]]
                if len(session_dates) == 0:
                    continue
                if status in ('untrainable', 'unbiasable') and len(session_dates) > 1:
                    # Add data point for first session only
                    data['data'].append({'x': time.mktime(session_dates[0].timetuple()),
                                         'y': subject_idx})
                    # Add data points to the previous status
                    all_prev = status_dates[status_dates < date_reached].sort_values(ascending=False)
                    prev_status = next(iter(all_prev.keys()), 'in_training')  # previous training status
                    # NB: Order is important here; 'untrainable' is processed last, after others added
                    prev_data = next(d for d in all_data['datasets'] if d['label'] == prev_status)
                    prev_data['data'].extend(
                        [{'x': time.mktime(d.timetuple()), 'y': subject_idx} for d in session_dates[1:]])
                else:
                    data['data'].extend([
                        {'x': time.mktime(d.timetuple()), 'y': subject_idx} for d in session_dates]
                    )
            all_data['datasets'].append(data)
        all_data['datasets'] = list(reversed(all_data['datasets']))
        for dset in all_data['datasets']:
            dset['data'] = sorted(dset['data'], key=lambda d: d['x'])
        return all_data

    @staticmethod
    def status_colour_map():
        """Return map of status: hexadecimal colour"""
        statuses = SubjectTrainingPlots.statuses
        gradient = np.linspace(0, 1, len(statuses))
        return {status: to_hex(plt.cm.hsv(i)) for i, status in zip(gradient, statuses)}


class SubjectFilter(django_filters.FilterSet):

    nickname = django_filters.ModelChoiceFilter(queryset=Subject.objects.all(), label='Nickname')
    lab = django_filters.ModelChoiceFilter(queryset=Lab.objects.all(), label='Lab')

    class Meta:
        model = Note
        fields = ['nickname', 'lab']
        exclude = ['json']

    def __init__(self, *args, **kwargs):
        super(SubjectFilter, self).__init__(*args, **kwargs)

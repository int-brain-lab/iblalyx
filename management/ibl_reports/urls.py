from django.urls import path
from django.conf import settings
from django.conf.urls.static import static

from . import views

urlpatterns = [
    path('', views.landingpage),
    path('overview', views.InsertionTable.as_view(), name='insertion table'),
    path('overview/<uuid:pid>', views.InsertionOverview.as_view(), name='insertion overview'),
    path('task_qc_eid/<uuid:eid>', views.plot_task_qc_eid, name='plot_task_qc_eid'),
    path('video_qc_eid/<uuid:eid>', views.plot_video_qc_eid, name='plot_video_qc_eid'),
    path('dlc_qc_eid/<uuid:eid>', views.plot_dlc_qc_eid, name='plot_dlc_qc_eid'),
    path('spikesorting', views.SpikeSortingTable.as_view(), name='spikesorting table'),
    path('plot_qc', views.add_plot_qc, name='add_plot_qc'),

    path('gallery/plots', views.GalleryPlotsOverview.as_view(), name='plot_overview'),
    path('gallery/sessions', views.SessionImportantPlots.as_view(), name='session_overview'),
    path('gallery/subjects', views.SubjectTrainingPlots.as_view(), name='subject_overview'),

    path('gallery/<uuid:eid>', views.GalleryOverviewView.as_view(), name='session'),
    path('gallery/<uuid:eid>/gallery', views.GallerySessionView.as_view(), name='gallery'),
    path('gallery/<uuid:eid>/qc', views.GallerySessionQcView.as_view(), name='qc'),
    path('gallery/<uuid:eid>/task', views.GalleryTaskView.as_view(), name='task'),
    path('gallery/<uuid:eid>/behaviour', views.GallerySubPlotSessionView.as_view(plot_type='behaviour'), name='behaviour'),
    path('gallery/<uuid:eid>/spikesorting', views.GallerySubPlotProbeView.as_view(plot_type='spikesorting'), name='spikesorting'),
    path('gallery/<uuid:eid>/rawephys', views.GallerySubPlotProbeView.as_view(plot_type='rawephys'), name='rawephys'),
    path('gallery/<uuid:eid>/histology', views.GallerySubPlotProbeView.as_view(plot_type='histology'), name='histology'),
    path('gallery/<uuid:eid>/video', views.GallerySubPlotSessionView.as_view(plot_type='video'), name='video'),

    path('gallery/<uuid:pid>/video', views.GallerySubPlotSessionView.as_view(plot_type='video'), name='video'),
]

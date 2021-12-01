from django.urls import path
from django.conf import settings
from django.conf.urls.static import static

from . import views

urlpatterns = [
    path('overview', views.InsertionTableWithFilter.as_view(), name='insertion table'),
    path('task_qc/<uuid:pid>', views.plot_task_qc, name='plot_task_qc'),
    path('video_qc/<uuid:pid>', views.plot_video_qc, name='plot_video_qc'),
    path('overview/<uuid:pid>', views.InsertionOverview.as_view(), name='insertion overview'),
    path('spikesorting', views.SpikeSortingTable.as_view(), name='spikesorting table'),
    path('gallery/', views.GalleryView.as_view(), name='photoview'),
    path('gallery/<uuid:eid>', views.GallerySessionView.as_view(), name='photoview_session'),
    path('gallery/<uuid:eid>/gallery', views.GallerySessionView.as_view(), name='gallery'),
    path('task_qc_eid/<uuid:eid>', views.plot_task_qc_eid, name='plot_task_qc_eid'),
    path('video_qc_eid/<uuid:eid>', views.plot_video_qc_eid, name='plot_video_qc_eid'),
    path('gallery/<uuid:eid>/qc', views.GallerySessionQcView.as_view(), name='qc'),
    path('gallery/<uuid:eid>/spikesorting', views.GallerySpikeSortingView.as_view(), name='spikesorting'),
    path('gallery/<uuid:eid>/rawephys', views.GalleryRawEphysView.as_view(), name='rawephys'),
    path('gallery/<uuid:eid>/histology', views.GalleryHistologyView.as_view(), name='histology'),
    path('gallery/<uuid:eid>/task', views.GalleryTaskView.as_view(), name='task'),
    path('gallery/booboo', views.SessionImportantPlots.as_view(), name='booboo')
]



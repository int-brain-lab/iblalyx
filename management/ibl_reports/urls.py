from django.urls import path

from . import views

urlpatterns = [
    path('overview', views.InsertionTableWithFilter.as_view(), name='insertion table'),
    path('task_qc/<uuid:pid>', views.plot_task_qc, name='plot_task_qc'),
    path('video_qc/<uuid:pid>', views.plot_video_qc, name='plot_video_qc'),
    path('overview/<uuid:pid>', views.InsertionOverview.as_view(), name='insertion overview'),
    path('spikesorting', views.SpikeSortingTable.as_view(), name='spikesorting table')
]

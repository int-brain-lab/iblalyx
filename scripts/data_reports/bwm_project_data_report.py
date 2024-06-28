from actions.models import Session
from django.db.models import F, Count, Sum, Q

sessions = Session.objects.filter(
    projects__name='ibl_neuropixel_brainwide_01',
    task_protocol__icontains='ephys')  # 987 sessions

sessions = sessions.annotate(
    dt=F('end_time')-F('start_time'),
    data_size=Sum('data_dataset_session_related__file_size'),
    ephys_size=Sum("data_dataset_session_related__file_size", filter=Q(data_dataset_session_related__dataset_type__name__startswith='ephysData.raw.')),
    video_size=Sum("data_dataset_session_related__file_size", filter=Q(data_dataset_session_related__dataset_type__name='_iblrig_Camera.raw')),
    spikes_size=Sum("data_dataset_session_related__file_size", filter=Q(data_dataset_session_related__dataset_type__name='spikes.times')),
)

sdata = sessions.aggregate(
    ntrials=Sum('n_trials'),
    time=Sum('dt'),
    data_size_bytes=Sum('data_size'),
    ephys_size_bytes=Sum('ephys_size'),
    video_size_bytes=Sum('video_size'),
    nspikes=Sum('spikes_size') / 8,
    nsubjects=Count('subject',  distinct=True)
)

print(f"{sessions.count()} sessions")
print(f"{sdata['nsubjects']:_} subjects")
print(f"{sdata['nspikes'] / 1e6:_.0f} M spikes")
print(f"{sdata['ntrials']:_} trials")

print(f"{sdata['time']} recording length")
print(f"{sdata['data_size_bytes'] / 1024 ** 4:.2f} Tb overall")
print(f"{sdata['ephys_size_bytes'] / 1024 ** 4:.2f} Tb ephys")
print(f"{sdata['video_size_bytes'] / 1024 ** 4:.2f} Tb video")



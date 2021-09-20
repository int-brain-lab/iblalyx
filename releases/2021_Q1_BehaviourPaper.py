## Behaviour paper
from data.models import Tag, Dataset, DatasetType

file_behaviour_sessions = '/home/olivier/Documents/PYTHON/00_IBL/ibldevtools/Alyx/2021_04_14_behaviour_paper_sessions.csv'
file_behaviour_sessions = '/home/ubuntu/2021_04_14_behaviour_paper_sessions.csv'
with open(file_behaviour_sessions) as fid:
    lines = fid.readlines()

dtypes = [
            'trials.feedback_times',
            'trials.feedbackType',
            'trials.intervals',
            'trials.choice',
            'trials.response_times',
            'trials.contrastLeft',
            'trials.contrastRight',
            'trials.probabilityLeft',
            'trials.stimOn_times',
            'trials.repNum',
            'trials.goCue_times',
            ]

eids = [line.split(',')[1].strip() for line in lines if len(line) > 36]
dataset_types = DatasetType.objects.filter(name__in=dtypes)
dsets = Dataset.objects.filter(session__in=eids, dataset_type__in=dataset_types)

tag, _ = Tag.objects.get_or_create(name="Behaviour Paper", protected=True, public=True)
tag.datasets.set(dsets)
Tag.objects.filter(name='Behaviour Paper').values_list('datasets__session').distinct().count()

'''
Code to assign a lab to perform histology
'''

from experiments.models import ProbeInsertion
from misc.models import Lab


def query_insertions():
    '''
    Find insertions that do not have alignment assigned to them
    :return:
    '''

    all_insertions = ProbeInsertion.objects.filter(
        session__task_protocol__icontains='_iblrig_tasks_ephysChoiceWorld',
        session__project__name='ibl_neuropixel_brainwide_01',
        session__subject__actions_sessions__procedures__name='Histology',
        session__json__IS_MOCK=False
    )

    insertions = all_insertions.filter(
        json__todo_alignment__isnull=True
        )
    return insertions, all_insertions


exclude_lab = ['hoferlab', 'churchlandlab']
labs = Lab.objects.all().exclude(name__in=exclude_lab)

insertions, all_insertions = query_insertions()  # Init
print(len(insertions))
len_previous = len(all_insertions)
while len(insertions) > 0:
    # Work on first insertion off the list
    insertion = insertions[0]

    # Find if there are other insertions from same session
    insertions_tochange = insertions.filter(session__id=insertion.session.id)

    # Compute which lab should be assigned
    for lab in labs:
        # Note:
        # churchlandlab == churchlandlab_ucla
        # mrsicflogellab == hoferlab

        if lab.name == 'churchlandlab_ucla':
            insertions_lab_done = all_insertions.filter(session__lab__name__icontains='churchlandlab')
            insertions_lab_assigned = all_insertions.filter(json__todo_alignment__icontains='churchlandlab')
            len_ins_total_lab = len(insertions_lab_done) + len(insertions_lab_assigned)

        elif lab.name == 'mrsicflogellab':
            len_lab_done = len(all_insertions.filter(session__lab__name=lab.name)) + \
                           len(all_insertions.filter(session__lab__name='hoferlab'))
            insertions_lab_assigned = all_insertions.filter(json__todo_alignment=lab.name)
            len_ins_total_lab = len_lab_done + len(insertions_lab_assigned)

        else:
            insertions_lab_done = all_insertions.filter(session__lab__name=lab.name)
            insertions_lab_assigned = all_insertions.filter(json__todo_alignment=lab.name)
            len_ins_total_lab = len(insertions_lab_done) + len(insertions_lab_assigned)

        # Find minimum and assign
        if len_ins_total_lab < len_previous:  # Should always go into this loop at first pass
            len_previous = len_ins_total_lab
            lab_assigned = lab.name

    # Change JSON field of insertions
    for pi in insertions_tochange:
        d = pi.json
        d['todo_alignment'] = lab_assigned
        pi.json = d
        pi.save()

    # See if any insertions remaining to be assigned
    insertions, all_insertions = query_insertions()
    print(len(insertions))

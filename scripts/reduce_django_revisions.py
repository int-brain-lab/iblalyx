import subprocess

# script to reduce the number of django_revisions sitting in the alyx db
# command that we are looking to automate: ./manage.py deleterevisions --days=700
# https://pypi.org/project/django-reversion/3.0.9/

if __name__ == "__main__":
    # ./manage.py deleterevisions --days=700

    # Overestimate of how many revisions are still in the db
    # ============================================
    # CHANGE THIS IF IS GOING TO BECOME A CRON JOB
    # ============================================
    oldest_revision_day = 700

    # Max number of days we are looking to keep of revisions
    revision_max_day = 670
    working_dir = '/var/www/alyx-dev/alyx/'

    # loop through 10 days at a time to keep the manage command from stalling
    while oldest_revision_day > revision_max_day:
        oldest_revision_day -= 10
        manage_args = 'deleterevisions --days=' + str(oldest_revision_day)
        subprocess.run(['python', 'manage.py'] + manage_args.split(), cwd=working_dir)
        
# This loads the initial fixtures for the ibl database.
# Assumes that iblalyx is in the same directory as the alyx repository.
for fixture in ../../iblalyx/fixtures/*.json; do
    echo "Loading fixture: $fixture"
    python manage.py loaddata "$fixture"
done
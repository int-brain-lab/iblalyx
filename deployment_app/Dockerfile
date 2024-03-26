FROM python:3.11-slim-bookworm

# Time zone autoconfig
ENV TZ=Europe/London
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Install services, packages and perform cleanup
RUN apt-get update && apt-get install -y \
    vim \
    git \
    bash \
    gcc \
    libpq-dev \
    virtualenv \
    && apt-get install -y --no-install-recommends dialog \
    && apt-get install -y --no-install-recommends openssh-server \
    && apt-get autoremove \
    && rm -rf /var/lib/apt/lists/*

RUN echo "root:Docker!" | chpasswd

# Clone repo and configure virtual environment
RUN git clone --branch master https://github.com/cortex-lab/alyx.git /var/www/alyx
# Best practice for configuring python venv
ENV VIRTUAL_ENV=/var/www/alyx/venv
RUN virtualenv ${VIRTUAL_ENV} --python=python3
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
WORKDIR /var/www/alyx
RUN pip install -r requirements.txt
RUN pip install gunicorn && pip install whitenoise && pip install dj-database-url
# create backup
RUN git clone --branch main https://github.com/int-brain-lab/iblalyx.git /home/iblalyx \
    && ln -s /home/iblalyx/management/ibl_reports /var/www/alyx/alyx/ibl_reports \
    && ln -s /home/iblalyx/management/ibl_reports/templates /var/www/alyx/alyx/templates/ibl_reports \
    && pip install -r /home/iblalyx/requirements.txt

# the DATETIME argument forces rebuilding the container from those layers
ARG DATETIME

COPY gunicorn.conf.py /var/www/alyx/gunicorn.conf.py
COPY settings.py-deploy /var/www/alyx/alyx/alyx/settings.py
COPY settings_lab.py-deploy /var/www/alyx/alyx/alyx/settings_lab.py
COPY settings_secret.py-deploy /var/www/alyx/alyx/alyx/settings_secret.py
RUN git pull \
    && git rev-parse HEAD \
    && git -C /home/iblalyx pull \
    && git -C /home/iblalyx rev-parse HEAD

WORKDIR /var/www/alyx/alyx
CMD /var/www/alyx/alyx/manage.py collectstatic --noinput && gunicorn --config /var/www/alyx/gunicorn.conf.py alyx.wsgi

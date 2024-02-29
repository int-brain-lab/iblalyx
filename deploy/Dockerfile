FROM python:3.11-slim-bookworm

# Time zone autoconfig
ENV TZ=Europe/London
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Install services, packages and perform cleanup
RUN apt-get update && apt-get install -y \
    apache2 \
    apache2-utils \
    libapache2-mod-wsgi-py3 \
    vim \
    git \
    bash \
    gcc \
    libpq-dev \
    virtualenv \
    awscli \
    certbot \
    python3-certbot-apache \
    wget \
    && apt-get autoremove \
    && rm -rf /var/lib/apt/lists/*

RUN echo "root:Docker!" | chpasswd

# Alyx application installation
RUN git clone --branch master https://github.com/cortex-lab/alyx.git /var/www/alyx
# Best practice for configuring python venv
ENV VIRTUAL_ENV=/var/www/alyx/venv
RUN virtualenv ${VIRTUAL_ENV} --python=python3
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
WORKDIR /var/www/alyx
RUN pip install -r requirements.txt
RUN pip install dj-database-url

# create reports application
RUN git clone --branch main https://github.com/int-brain-lab/iblalyx.git /home/iblalyx \
    && ln -s /home/iblalyx/management/ibl_reports /var/www/alyx/alyx/ibl_reports \
    && ln -s /home/iblalyx/management/ibl_reports/templates /var/www/alyx/alyx/templates/ibl_reports \
    && pip install -r /home/iblalyx/requirements.txt

# Apache ENVs
ENV APACHE_RUN_USER www-data
ENV APACHE_RUN_GROUP www-data
ENV APACHE_LOCK_DIR /var/lock/apache2
ENV APACHE_LOG_DIR /var/log/apache2
ENV APACHE_PID_FILE /var/run/apache2/apache2.pid

# Apache configs
COPY apache-conf-alyx /etc/apache2/apache2.conf
COPY 000-default-conf-alyx /etc/apache2/sites-available/000-default.conf
RUN a2enmod rewrite
RUN a2enmod ssl
RUN a2enmod wsgi

# the DATETIME argument forces rebuilding the container from those layers
ARG DATETIME
COPY settings.py-deploy /var/www/alyx/alyx/alyx/settings.py
COPY settings_lab.py-deploy /var/www/alyx/alyx/alyx/settings_lab.py
COPY settings_secret.py-deploy /var/www/alyx/alyx/alyx/settings_secret.py
RUN git pull \
    && git rev-parse HEAD \
    && git -C /home/iblalyx pull \
    && git -C /home/iblalyx rev-parse HEAD

WORKDIR /var/www/alyx/alyx

# the entrypoint here just starts a local development server, this is overriden by the compse file
CMD env | grep _ >> /etc/environment && python /var/www/alyx/alyx/manage.py runserver 0.0.0.0:8000

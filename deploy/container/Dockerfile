FROM ubuntu/apache2:latest

# python unbuffered allows to get real-time logs
ENV PYTHONUNBUFFERED 1
ENV TZ=Europe/London
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Install services, packages and perform cleanup
RUN apt-get update && apt-get install -y \
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
    python3.11 \
    python3-pip \
    python3-venv \
    wget \
    w3m \
    && apt-get autoremove \
    && rm -rf /var/lib/apt/lists/*

RUN echo "root:Docker!" | chpasswd

# Alyx application installation
RUN git clone --branch master https://github.com/cortex-lab/alyx.git /var/www/alyx
# Best practice for configuring python venv
ENV VIRTUAL_ENV=/var/www/alyx/venv
RUN virtualenv ${VIRTUAL_ENV} --python=python3.11
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
WORKDIR /var/www/alyx
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir dj-database-url

# create reports application
RUN git clone --branch main https://github.com/int-brain-lab/iblalyx.git /home/iblalyx \
    && ln -s /home/iblalyx/management/ibl_reports /var/www/alyx/alyx/ibl_reports \
    && ln -s /home/iblalyx/management/ibl_reports/templates /var/www/alyx/alyx/templates/ibl_reports \
    && pip install --no-cache-dir -r /home/iblalyx/requirements.txt

# Apache ENVs
ENV APACHE_RUN_USER www-data
ENV APACHE_RUN_GROUP www-data
ENV APACHE_LOCK_DIR /var/lock/apache2
ENV APACHE_LOG_DIR /var/log/apache2
ENV APACHE_PID_FILE /var/run/apache2/apache2.pid

RUN mkdir -p ${APACHE_LOG_DIR} \
    && touch ${APACHE_LOG_DIR}/django.log \
    && touch ${APACHE_LOG_DIR}/django.json \
    && chown -R ${APACHE_RUN_USER}:${APACHE_RUN_GROUP} ${APACHE_LOG_DIR}

# Apache configs
COPY apache-conf-alyx /etc/apache2/apache2.conf
COPY 000-default-conf-alyx /etc/apache2/sites-available/000-default.conf
RUN a2enmod rewrite
RUN a2enmod ssl
RUN a2enmod wsgi

# the DATETIME argument forces rebuilding the container from those layers
ARG DATETIME
COPY settings-deploy.py /var/www/alyx/alyx/alyx/settings.py
COPY settings_lab-deploy.py /var/www/alyx/alyx/alyx/settings_lab.py
RUN git pull \
    && git rev-parse HEAD \
    && git -C /home/iblalyx pull \
    && git -C /home/iblalyx rev-parse HEAD

WORKDIR /var/www/alyx/alyx

# the entrypoint here just starts a local development server, this is overriden by the compse file
CMD python /var/www/alyx/alyx/manage.py collectstatic --noinput && python /var/www/alyx/alyx/manage.py runserver 0.0.0.0:8000

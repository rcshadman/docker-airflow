# VERSION 1.7.1.3
# AUTHOR: Matthieu "Puckel_" Roisil
# DESCRIPTION: Basic Airflow container
# BUILD: docker build --rm -t puckel/docker-airflow
# SOURCE: https://github.com/puckel/docker-airflow

FROM debian:jessie
MAINTAINER flolas

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Airflow
ARG AIRFLOW_VERSION=1.7.1.3
ENV AIRFLOW_HOME /usr/local/airflow

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8
ENV LC_ALL  en_US.UTF-8

RUN set -ex \
    && buildDeps=' \
        python-pip \
        python-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
	libmysqlclient-dev \
        build-essential \
        libblas-dev \
        liblapack-dev \
        alien \
        pdksh \
        dpkg-dev \
        debhelper \
        libatlas-base-dev \
    ' \
    && echo "deb http://http.debian.net/debian jessie-backports main" >/etc/apt/sources.list.d/backports.list \
    && apt-get update -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        apt-utils \
        curl \
        netcat \
        locales \
	freetds-dev \
	default-jre \
    && apt-get install -yqq -t jessie-backports python-requests libpq-dev \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow

RUN  pip install pytz==2015.7 \
    && pip install cryptography \
    && pip install pyOpenSSL \
    && pip install ndg-httpsclient \
    && pip install pyasn1 \
    && pip install JayDeBeApi \
    && pip install teradata \
    && pip install docker-py \
    && pip install psycopg2
RUN apt-get install -yqq freetds-dev
RUN apt-get install -y libatlas-base-dev
RUN easy_install atlas
RUN pip install -U pip && pip -v install airflow[docker,celery,postgres,hive,mysql,jdbc,mssql,crypto,gcp_api,hdfs,password]==$AIRFLOW_VERSION \
    && apt-get remove --purge -yqq $buildDeps libpq-dev \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

# Install teradata python module dependencies
RUN mkdir /opt/teradata_odbc
COPY teradata_odbc/TeraGSS_linux_x64-15.10.01.01-1.noarch.rpm /opt/teradata_odbc
COPY teradata_odbc/tdicu1510-15.10.01.00-1.noarch.rpm /opt/teradata_odbc
COPY teradata_odbc/tdodbc1510-15.10.01.01-1.noarch.rpm  /opt/teradata_odbc
RUN  alien -i /opt/teradata_odbc/TeraGSS_linux_x64-15.10.01.01-1.noarch.rpm --scripts && \
     alien -i /opt/teradata_odbc/tdicu1510-15.10.01.00-1.noarch.rpm --scripts && \
     alien -i /opt/teradata_odbc/tdodbc1510-15.10.01.01-1.noarch.rpm --scripts
ENV ODBCINI=/opt/teradata/client/15.10/odbc_64/odbc.ini
RUN ln -sfn /opt/teradata/client/15.10/lib64/libicudatatd.so.52.1 /usr/lib/libicudatatd.so;\
ln -sfn /opt/teradata/client/15.10/lib64/libicudatatd.so.52.1 /usr/lib/libicudatatd.so.52; \
ln -sfn /opt/teradata/client/15.10/lib64/libicui18ntd.so.52.1 /usr/lib/libicui18ntd.so;\
ln -sfn /opt/teradata/client/15.10/lib64/libicui18ntd.so.52.1 /usr/lib/libicui18ntd.so.52 ;\
ln -sfn /opt/teradata/client/15.10/lib64/libicuiotd.so.52.1 /usr/lib/libicuiotd.so;\
ln -sfn /opt/teradata/client/15.10/lib64/libicuiotd.so.52.1 /usr/lib/libicuiotd.so.52;\
ln -sfn /opt/teradata/client/15.10/lib64/libiculetd.so.52.1 /usr/lib/libiculetd.so;\
ln -sfn /opt/teradata/client/15.10/lib64/libiculetd.so.52.1 /usr/lib/libiculetd.so.52;\
ln -sfn /opt/teradata/client/15.10/lib64/libiculxtd.so.52.1 /usr/lib/libiculxtd.so;\
ln -sfn /opt/teradata/client/15.10/lib64/libiculxtd.so.52.1 /usr/lib/libiculxtd.so.52;\
ln -sfn /opt/teradata/client/15.10/lib64/libicuuctd.so.52.1 /usr/lib/libicuuctd.so;\
ln -sfn /opt/teradata/client/15.10/lib64/libicuuctd.so.52.1 /usr/lib/libicuuctd.so.52;\
ln -sfn /opt/teradata/client/15.10/lib64/libivicu27.so /usr/lib/libivicu27.so;\
ln -sfn /opt/teradata/client/15.10/lib64/libodbc.so /usr/lib/libodbc.so;\
ln -sfn /opt/teradata/client/15.10/lib64/libodbcinst.so /usr/lib/libodbcinst.so;\
ln -sfn /opt/teradata/client/15.10/lib64/odbccurs.so /usr/lib/odbccurs.so;\
ln -sfn /opt/teradata/client/15.10/lib64/vscnctdlg.so /usr/lib/vscnctdlg.so;\
ln -sfn /opt/teradata/client/15.10/lib64/libddicu27.so /usr/lib/libddicu27.so

RUN  pip install lxml python-nvd3
# Teradata Configuration
ARG TD_VERSION=15.10
ARG TD_CLIENT_PATH=/opt/teradata/client

# ODBC TDICU
ENV ODBCINST $TD_CLIENT_PATH/$TD_VERSION/odbc_64/odbcinst.ini
ENV ODBCINI  $TD_CLIENT_PATH/$TD_VERSION/odbc_64/odbc.ini
ENV TD_ICU_DATA $TD_CLIENT_PATH/$TD_VERSION/tdicu/lib64
ENV MANPATH $TD_CLIENT_PATH/$TD_VERSION/odbc_64/help/man:$MANPATH
ENV NLSPATH $TD_CLIENT_PATH/$TD_VERSION/odbc_64/msg/%N:$NLSPATH
ENV COPLIB $TD_CLIENT_PATH/$TD_VERSION/lib64
ENV COPERR $TD_CLIENT_PATH/$TD_VERSION/lib64
ENV LD_LIBRARY_PATH $TD_CLIENT_PATH/$TD_VERSION/lib64:/usr/lib64

COPY script/entrypoint.sh ${AIRFLOW_HOME}/entrypoint.sh
COPY config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

COPY mysql_hook.py /usr/local/lib/python2.7/dist-packages/airflow/hooks/
COPY teradata_hook.py /usr/local/lib/python2.7/dist-packages/airflow/contrib/hooks/
COPY docker_operator.py /usr/local/lib/python2.7/dist-packages/airflow/operators/
COPY hipchat_operator.py /usr/local/lib/python2.7/dist-packages/airflow/contrib/operators/
COPY views.py /usr/local/lib/python2.7/dist-packages/airflow/www/
COPY odbc.ini /opt/teradata/client/15.10/odbc_64/

RUN mkdir ${AIRFLOW_HOME}/files

RUN chown -R airflow: ${AIRFLOW_HOME} \
    && chmod +x ${AIRFLOW_HOME}/entrypoint.sh

EXPOSE 8080 5555 8793

USER airflow
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["./entrypoint.sh"]

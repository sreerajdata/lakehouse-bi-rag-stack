FROM apache/airflow:2.8.1

USER airflow

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io git openjdk-17-jre-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-apache-kafka \
    apache-airflow-providers-trino \
    great-expectations \
    confluent-kafka \
    pandas \
    faker \
    pyspark==3.4.3 \
    dbt-trino==1.7.1 \
    "acryl-datahub[dbt,trino]==0.14.1"

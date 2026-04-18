FROM apache/airflow:2.8.1

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
    dbt-trino==1.7.1

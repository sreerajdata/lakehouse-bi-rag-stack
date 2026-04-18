FROM python:3.11-slim

WORKDIR /usr/app/dbt

RUN pip install --no-cache-dir dbt-trino==1.7.1

CMD ["tail", "-f", "/dev/null"]

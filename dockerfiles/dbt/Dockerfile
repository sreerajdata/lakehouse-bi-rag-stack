FROM python:3.11-slim

WORKDIR /usr/app/dbt

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir dbt-trino==1.7.1

CMD ["tail", "-f", "/dev/null"]

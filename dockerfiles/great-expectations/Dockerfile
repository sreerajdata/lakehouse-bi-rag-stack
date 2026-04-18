FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir \
    "great_expectations==0.18.21" \
    "sqlalchemy==1.4.52" \
    "trino[sqlalchemy]==0.336.0" \
    "pandas==2.2.3"

WORKDIR /workspace

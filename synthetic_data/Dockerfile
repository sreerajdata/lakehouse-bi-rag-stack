FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    psycopg2-binary==2.9.9 \
    faker==22.0.0 \
    pandas==2.1.4

COPY generator.py .

CMD ["python", "-u", "generator.py"]

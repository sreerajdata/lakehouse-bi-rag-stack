FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir boto3 Flask requests

COPY app.py /app/app.py

EXPOSE 8888

CMD ["python", "/app/app.py"]

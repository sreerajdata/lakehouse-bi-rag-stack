FROM jupyter/datascience-notebook:latest

# Switch to root to install system-level packages
USER root

# Install system dependency for pyiceberg/arrow
RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends libsnappy-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch back to the notebook user for pip installs
USER ${NB_UID}

# Bake in all data-platform libraries at build time so notebook spawn
# is instant and does not require internet access at runtime.
RUN pip install --no-cache-dir \
    trino==0.328.0 \
    pyiceberg[s3fs,pyarrow]==0.7.1 \
    "sqlalchemy-trino>=0.5.0" \
    boto3==1.34.84 \
    pandas==2.2.2 \
    pyarrow==15.0.2 \
    confluent-kafka==2.4.0 \
    milvus-model==0.2.7 \
    pymilvus==2.4.3 \
    httpx==0.27.0 \
    dbt-core==1.8.3 \
    dbt-trino==1.8.2

# Pre-configure Trino connection defaults via ipython startup script
RUN mkdir -p /home/${NB_USER}/.ipython/profile_default/startup
COPY trino_startup.py /home/${NB_USER}/.ipython/profile_default/startup/00-trino-defaults.py

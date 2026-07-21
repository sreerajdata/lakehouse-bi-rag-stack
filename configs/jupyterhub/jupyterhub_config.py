c = get_config()

c.JupyterHub.bind_url = "http://0.0.0.0:8000"
c.JupyterHub.spawner_class = "simple"
c.Authenticator.admin_users = {"admin"}
c.Authenticator.allowed_users = {"admin", "analyst", "data_engineer"}
c.PAMAuthenticator.open_sessions = False

c.Spawner.default_url = "/lab"
c.Spawner.environment = {
    # Data platform endpoints — all pre-wired to lakehouse_net services
    "TRINO_HOST":            "trino",
    "TRINO_PORT":            "8080",
    "TRINO_CATALOG":         "iceberg",
    "S3_ENDPOINT":           "http://seaweedfs-s3:8333",
    "AWS_ACCESS_KEY_ID":     "admin",
    "AWS_SECRET_ACCESS_KEY": "admin123",
    "KAFKA_BOOTSTRAP":       "kafka:9092",
    "OLLAMA_URL":            "http://ollama:11434",
    "MILVUS_HOST":           "milvus",
    "MILVUS_PORT":           "19530",
}
# NOTE: trino, pyiceberg, boto3, pymilvus, confluent-kafka, dbt-trino and other
# data-platform libraries are baked into the image at build time via
# dockerfiles/jupyterhub/Dockerfile — no runtime pip install needed.

c = get_config()  # noqa

c.JupyterHub.bind_url = "http://0.0.0.0:8000"
c.JupyterHub.spawner_class = "simple"
c.Authenticator.admin_users = {"admin"}
c.Authenticator.allowed_users = {"admin", "analyst", "data_engineer"}
c.PAMAuthenticator.open_sessions = False

# Single-user server settings
c.Spawner.default_url = "/lab"
c.Spawner.environment = {
    "TRINO_HOST":           "trino",
    "TRINO_PORT":           "8080",
    "S3_ENDPOINT":          "http://seaweedfs-s3:8333",
    "AWS_ACCESS_KEY_ID":    "admin",
    "AWS_SECRET_ACCESS_KEY": "admin123",
    "KAFKA_BOOTSTRAP":      "kafka:9092",
    "OLLAMA_URL":           "http://ollama:11434",
}

import os
from flask_appbuilder.security.manager import AUTH_DB

SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "supersecret123")
SQLALCHEMY_DATABASE_URI = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://admin:admin123@postgres:5432/superset"
)

# Trino / Iceberg connection via SQLAlchemy
ADDITIONAL_DATABASES = {
    "trino_iceberg": {
        "sqlalchemy_uri": "trino://admin@trino:8080/iceberg",
        "name": "TPL Lakehouse (Trino/Iceberg)",
    }
}

AUTH_TYPE = AUTH_DB
WTF_CSRF_ENABLED = True
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DRILL_TO_DETAIL": True,
    "DATAPANEL_CLOSED_BY_DEFAULT": False,
}

# Row-level security
ENABLE_ROW_LEVEL_SECURITY = True
ROW_LEVEL_SECURITY_FILTER_TYPE = "Regular"

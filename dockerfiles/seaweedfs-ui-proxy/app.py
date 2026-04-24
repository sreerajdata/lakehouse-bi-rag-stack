import html
import os
from urllib.parse import quote

import boto3
import requests
from flask import Flask, Response, jsonify, request


app = Flask(__name__)

FILER_BASE = os.environ.get("FILER_BASE", "http://seaweedfs-filer:8888")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://seaweedfs-s3:8333")
S3_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
S3_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "admin123")

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
)


def wants_json() -> bool:
    accept = request.headers.get("Accept", "")
    return "application/json" in accept


def split_path(path: str) -> list[str]:
    return [segment for segment in path.split("/") if segment]


def collect_child_names(bucket: str, prefix: str) -> list[str]:
    names: set[str] = set()
    continuation_token = None

    while True:
        params = {
            "Bucket": bucket,
            "Prefix": prefix,
            "Delimiter": "/",
            "MaxKeys": 1000,
        }
        if continuation_token:
            params["ContinuationToken"] = continuation_token

        response = s3.list_objects_v2(**params)

        for common_prefix in response.get("CommonPrefixes", []):
            value = common_prefix.get("Prefix", "")
            if value.startswith(prefix):
                name = value[len(prefix):].strip("/")
                if name:
                    names.add(name)

        for entry in response.get("Contents", []):
            key = entry.get("Key", "")
            if not key.startswith(prefix):
                continue
            rest = key[len(prefix):].strip("/")
            if not rest:
                continue
            name = rest.split("/", 1)[0]
            if name:
                names.add(name)

        if not response.get("IsTruncated"):
            break
        continuation_token = response.get("NextContinuationToken")

    return sorted(names)


def has_descendants(bucket: str, prefix: str) -> bool:
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=10)
    for entry in response.get("Contents", []):
        key = entry.get("Key", "")
        if key != prefix:
            return True
    return bool(response.get("CommonPrefixes"))


def warehouse_entries(bucket: str) -> list[str]:
    entries = []
    for name in collect_child_names(bucket, "warehouse/"):
        prefix = f"warehouse/{name}/"
        if has_descendants(bucket, prefix):
            entries.append(name)
    return entries


def db_entries(bucket: str, db_name: str) -> list[str]:
    prefix = f"warehouse/{db_name}/"
    return [
        name
        for name in collect_child_names(bucket, prefix)
        if "__dbt_tmp-" not in name
    ]


def breadcrumb_html(parts: list[tuple[str, str]]) -> str:
    items = "".join(
        f'<li><a href="{html.escape(url)}">{html.escape(label)}</a></li>'
        for label, url in parts
    )
    return f'<ol class="breadcrumb">{items}</ol>'


def render_html(title: str, rows: list[tuple[str, str]]) -> str:
    row_html = "".join(
        (
            "<tr>"
            "<td><span class=\"folder\">&#128193;</span>"
            f"<a href=\"{html.escape(url)}\">{html.escape(name)}</a></td>"
            "</tr>"
        )
        for name, url in rows
    )
    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; color: #222; }}
    h1 {{ margin-bottom: 12px; }}
    .breadcrumb {{ list-style: none; padding: 0; margin: 0 0 18px; display: flex; gap: 8px; flex-wrap: wrap; }}
    .breadcrumb li::after {{ content: '/'; margin-left: 8px; color: #888; }}
    .breadcrumb li:last-child::after {{ content: ''; }}
    a {{ color: #0b57d0; text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    table {{ width: 100%; border-collapse: collapse; }}
    td {{ border-top: 1px solid #ddd; padding: 12px 6px; }}
    .folder {{ margin-right: 8px; }}
    .empty {{ color: #666; font-style: italic; padding-top: 8px; }}
  </style>
</head>
<body>
  <h1>SeaweedFS Browser</h1>
  {title}
  <table>{row_html}</table>
  {'' if rows else '<div class="empty">No folders to display.</div>'}
</body>
</html>"""


def render_json(path: str, names: list[str]) -> Response:
    entries = []
    for name in names:
        entries.append(
            {
                "FullPath": f"{path.rstrip('/')}/{name}",
                "FileSize": 0,
                "Mime": "application/x-directory",
            }
        )
    return jsonify({"Path": path.rstrip("/"), "Entries": entries, "EmptyFolder": not entries})


def warehouse_response(bucket: str):
    names = warehouse_entries(bucket)
    path = f"/buckets/{bucket}/warehouse/"
    if wants_json():
        return render_json(path, names)

    crumbs = breadcrumb_html(
        [
            ("/", "/"),
            ("buckets", "/buckets/"),
            (bucket, f"/buckets/{quote(bucket)}/"),
            ("warehouse", path),
        ]
    )
    rows = [(name, f"{path}{quote(name)}/") for name in names]
    return Response(render_html(crumbs, rows), mimetype="text/html")


def db_response(bucket: str, db_name: str):
    names = db_entries(bucket, db_name)
    path = f"/buckets/{bucket}/warehouse/{db_name}/"
    if wants_json():
        return render_json(path, names)

    crumbs = breadcrumb_html(
        [
            ("/", "/"),
            ("buckets", "/buckets/"),
            (bucket, f"/buckets/{quote(bucket)}/"),
            ("warehouse", f"/buckets/{quote(bucket)}/warehouse/"),
            (db_name, path),
        ]
    )
    rows = [(name, f"{path}{quote(name)}/") for name in names]
    return Response(render_html(crumbs, rows), mimetype="text/html")


def proxy_request(path: str) -> Response:
    upstream_url = f"{FILER_BASE}/{path}"
    if request.query_string:
        upstream_url = f"{upstream_url}?{request.query_string.decode()}"

    headers = {
        key: value
        for key, value in request.headers.items()
        if key.lower() not in {"host", "content-length"}
    }

    upstream = requests.request(
        method=request.method,
        url=upstream_url,
        headers=headers,
        data=request.get_data(),
        cookies=request.cookies,
        allow_redirects=False,
        timeout=60,
    )

    response_headers = [
        (key, value)
        for key, value in upstream.headers.items()
        if key.lower() not in {"content-length", "transfer-encoding", "connection", "content-encoding"}
    ]
    return Response(upstream.content, upstream.status_code, response_headers)


@app.route("/", defaults={"path": ""}, methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])
@app.route("/<path:path>", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])
def catch_all(path: str):
    normalized = f"/{path}".rstrip("/")
    parts = split_path(normalized)

    if request.method == "GET" and len(parts) == 3 and parts[0] == "buckets" and parts[2] == "warehouse":
        return warehouse_response(parts[1])

    if (
        request.method == "GET"
        and len(parts) == 4
        and parts[0] == "buckets"
        and parts[2] == "warehouse"
        and parts[3].endswith(".db")
    ):
        return db_response(parts[1], parts[3])

    return proxy_request(path)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8888)

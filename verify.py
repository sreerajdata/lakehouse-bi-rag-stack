import urllib.request
import json
import base64

def check_grafana():
    url = "http://localhost:3000/api/datasources"
    request = urllib.request.Request(url)
    auth = base64.b64encode(b"admin:admin123").decode("ascii")
    request.add_header("Authorization", f"Basic {auth}")
    try:
        with urllib.request.urlopen(request) as response:
            data = json.loads(response.read().decode())
            for ds in data:
                print(f"Grafana Datasource: ID={ds['id']}, UID={ds['uid']}, Name={ds['name']}, Type={ds['type']}")
    except Exception as e:
        print("Grafana Error:", e)

def check_nifi():
    url = "http://localhost:8090/nifi-api/flow/process-groups/root"
    try:
        with urllib.request.urlopen(url) as response:
            data = json.loads(response.read().decode())
            status = data['processGroupFlow']['flow']['processGroups'][0]['status']['aggregateSnapshot']
            print("NiFi FlowFiles Queued:", status.get('queuedCount', 0))
    except Exception as e:
        print("NiFi Error:", e)

if __name__ == "__main__":
    check_grafana()
    check_nifi()

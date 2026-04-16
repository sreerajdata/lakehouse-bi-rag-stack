Write-Host "Layer 1: Core"
docker compose --env-file .env --profile core up -d
Start-Sleep -Seconds 20
try {
    $response = Invoke-WebRequest -Uri "http://localhost:9333/cluster/status" -ErrorAction Stop
    Write-Host "SeaweedFS OK"
} catch {
    Write-Host "SeaweedFS failed: $($_.Exception.Message)"
}

$pg_status = docker exec lakehouse_postgres pg_isready -U admin
if ($LASTEXITCODE -eq 0) { Write-Host "Postgres OK" } else { Write-Host "Postgres NOT OK" }

Write-Host "Layer 2: Initialize buckets"
docker compose --env-file .env --profile core run --rm seaweedfs-init
Start-Sleep -Seconds 5

Write-Host "Layer 3: Ingestion"
docker compose --env-file .env --profile ingestion up -d
Start-Sleep -Seconds 30
try {
    $response = Invoke-WebRequest -Uri "http://localhost:9000" -ErrorAction Stop
    Write-Host "Kafka UI OK"
} catch {
    Write-Host "Kafka UI not ready yet"
}

Write-Host "Layer 4: Synthetic data"
docker compose --env-file .env --profile synthetic up -d
Start-Sleep -Seconds 10
docker logs lakehouse_synthetic_datagen --tail 20

Write-Host "Layer 5: Processing"
docker compose --env-file .env --profile processing up -d
Start-Sleep -Seconds 60
try {
    $response = Invoke-WebRequest -Uri "http://localhost:8181" -ErrorAction Stop
    Write-Host "Spark OK"
} catch {
    Write-Host "Spark starting..."
}

try {
    $response = Invoke-WebRequest -Uri "http://localhost:8280/health" -ErrorAction Stop
    Write-Host "Airflow OK"
} catch {
    Write-Host "Airflow starting..."
}

Write-Host "Layer 6: Lakehouse"
docker compose --env-file .env --profile lakehouse up -d
Start-Sleep -Seconds 40
try {
    $response = Invoke-WebRequest -Uri "http://localhost:8180/v1/info" -ErrorAction Stop
    Write-Host "Trino OK"
} catch {
    Write-Host "Trino starting..."
}

Write-Host "Layer 7: Monitoring"
docker compose --env-file .env --profile monitoring up -d
Start-Sleep -Seconds 15

Write-Host "Layer 8: Analytics"
docker compose --env-file .env --profile analytics up -d
Start-Sleep -Seconds 20

Write-Host "Layer 9: AI"
docker compose --env-file .env --profile ai up -d
Start-Sleep -Seconds 5

Write-Host "Printing Docker Status"
docker compose --profile all ps --format "table {{.Name}}`t{{.Status}}`t{{.Ports}}"

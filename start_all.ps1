docker compose --profile all down -v --remove-orphans
docker compose --env-file .env --profile core up -d
Start-Sleep -Seconds 30
docker compose --env-file .env --profile core run --rm seaweedfs-init
Start-Sleep -Seconds 10
docker compose --env-file .env --profile core --profile ingestion up -d
Start-Sleep -Seconds 45
docker compose --env-file .env --profile core --profile ingestion --profile synthetic up -d
Start-Sleep -Seconds 15
docker compose --env-file .env --profile core --profile ingestion --profile synthetic --profile processing up -d
Start-Sleep -Seconds 90
docker compose --env-file .env --profile core --profile ingestion --profile synthetic --profile processing --profile lakehouse up -d
Start-Sleep -Seconds 60
docker compose --env-file .env --profile core --profile ingestion --profile synthetic --profile processing --profile lakehouse --profile monitoring up -d
Start-Sleep -Seconds 20
docker compose --env-file .env --profile core --profile ingestion --profile synthetic --profile processing --profile lakehouse --profile monitoring --profile analytics up -d
Start-Sleep -Seconds 30
docker compose --env-file .env --profile core --profile ingestion --profile synthetic --profile processing --profile lakehouse --profile monitoring --profile analytics --profile ai up -d
Start-Sleep -Seconds 30
docker compose --env-file .env --profile core --profile ingestion --profile synthetic --profile processing --profile lakehouse --profile monitoring --profile analytics --profile ai --profile governance up -d
Start-Sleep -Seconds 30
docker compose --env-file .env --profile all up -d
Start-Sleep -Seconds 30

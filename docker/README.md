# Docker Deployment Guide

## Prerequisites
- Docker (version 20.10.0+)
- Docker Compose (version v2.0.0+)

## Quick Start
1. Ensure `.env` is populated at project root.
2. Build images: `docker-compose build`
3. Start services: `docker-compose up -d`
4. The Postgres DB will initialize. Wait until logs say database is ready.
5. The pipeline service will start and execute orchestration tasks automatically.

### Verifying Services
```bash
docker-compose ps
```

### Accessing Database
```bash
docker exec -it ecommerce-data-pipeline_postgres_1 psql -U admin -d ecommerce_db
```

### Viewing Logs
```bash
docker-compose logs -f pipeline
```

### Cleanup
```bash
docker-compose down -v
```

## Troubleshooting
- **Port already in use**: Postgres port 5432 is engaged. End existing PG instances.
- **Database not ready**: Pipeline exits if it can't reach postgres. Ensure `depends_on` healthy is met.

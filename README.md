# Apache Iceberg Course - Docker Setup

This Docker setup provides a complete, production-like environment for learning Apache Iceberg with:
- **MinIO**: A Local S3-compatible object storage for table data
- **Polaris**: Apache Iceberg REST Catalog
- **Jupyter Notebook**: Interactive Python notebook with PySpark and Iceberg support
- **Trino**: Distributed SQL query engine

You should have found this repositories along with the course videos here (TODO LINK), please check them out if you haven't.

## Version Configuration

All versions are centrally managed in the `.env` file:

Current pinned versions:

- **Iceberg**: 1.10.0 (released September 5, 2024)
- **Spark**: 4.0.1 with Scala 2.13 (May 23, 2024)
- **Polaris**: latest
- **Trino**: 465

To update versions, simply edit the `.env` file and rebuild:

```bash
docker-compose up -d --build
```

## Prerequisites

- Docker Desktop installed and running
- At least 8GB of RAM allocated to Docker
- At least 10GB of free disk space

## Quick Start

1. **Start all services**:
   ```bash
   docker-compose up -d
   ```

2. **Wait for services to be ready** (approximately 1-2 minutes):
   ```bash
   docker-compose logs -f
   ```
   Press `Ctrl+C` to stop following logs once services are running.

3. **Access the services**:
   - **Jupyter Notebook**: http://localhost:8888 (no password) - **Start here!**
   - **MinIO Console**: http://localhost:9001 (admin/password) - View your data
   - **Trino UI**: http://localhost:8080
   - **Polaris API**: http://localhost:8181

4. **Open the demo notebook**:
- Direct link: http://localhost:8888/lab/tree/work/E1.1%20-%20OpenLakehouse.ipynb
   - Run through the cells to see Iceberg with Polaris and MinIO

## Service Details

### MinIO (S3-Compatible Storage)

MinIO provides S3-compatible object storage for Iceberg table data.

**Configuration**:
- **API Port**: 9000
- **Console Port**: 9001
- **Username**: admin
- **Password**: password
- **Bucket**: warehouse
- **Data directory**: `./data/minio`

**Access the Console**:
- URL: http://localhost:9001
- Login with admin/password
- Browse the `warehouse` bucket to see your Iceberg table files

### Polaris Iceberg REST Catalog

The Polaris catalog provides a REST API for managing Iceberg table metadata. It's configured with in-memory persistence for Catalog entries and MinIO for table metadata.

**Configuration**:
- Port: 8181
- Data directory: `./data/polaris`
- Storage: MinIO S3 (s3://warehouse/)
- OAuth2 credentials automatically generated on first start

**Initialization**:
The Polaris catalog is automatically initialized with:
- S3 storage configuration pointing to MinIO
- OAuth2 credentials (root:s3cr3t defined in .env)

The `polaris-setup` service runs `bootstrap-catalog.sh` on startup to configure the catalog.

### Trino

Trino is configured with an Iceberg connector that connects to the Polaris catalog.

**Configuration**:
- Port: 8080
- Catalog: `iceberg` (connected to Polaris)
- Data directory: `./data/trino`
- Config files: `./trino/config/`

**Connect to Trino CLI**:
```bash
docker exec -it trino trino
```

**Example Trino queries**:
```sql
-- Show catalogs
SHOW CATALOGS;

-- Create a namespace
CREATE SCHEMA iceberg.demo;

-- Show schemas
SHOW SCHEMAS IN iceberg;

-- Create a table
CREATE TABLE iceberg.demo.test (
    id BIGINT,
    name VARCHAR
) WITH (format = 'PARQUET');

-- Insert data
INSERT INTO iceberg.demo.test VALUES (1, 'Alice'), (2, 'Bob');

-- Query data
SELECT * FROM iceberg.demo.test;
```

### Jupyter Notebook with PySpark

The Jupyter environment comes pre-configured with:
- PySpark with Iceberg support
- PyIceberg library
- Trino Python client
- Pandas, Matplotlib, Seaborn

**Access**:
- URL: http://localhost:8888
- Notebooks directory: `./notebooks`
- Data directory: `./data/jupyter`

**Catalog Configuration**:
- The demo notebook uses the **Polaris REST catalog** with **MinIO S3 storage**
- Metadata managed by Polaris (centralized, REST API)
- Table data stored in MinIO (s3://warehouse/)
- **This is a production-like pattern** - same architecture as using Polaris with real S3/Azure/GCS
- OAuth2 authentication configured automatically

**Sample notebook**: `E1.1 - OpenLakehouse.ipynb` is provided with examples of:
- Creating Iceberg tables
- Querying data
- ACID transactions
- Time travel
- Schema evolution
- Partitioning

## Data Persistence

All data is stored locally in the `./data` directory:
- `./data/minio`: MinIO object storage (Iceberg table data)
- `./data/polaris`: Polaris catalog metadata
- `./data/trino`: Trino working data
- `./data/jupyter`: Jupyter user data

This ensures that your data persists even when containers are stopped.

## Common Commands

**Start all services**:
```bash
docker-compose up -d
```

**Stop all services**:
```bash
docker-compose down
```

**View logs**:
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f jupyter
docker-compose logs -f trino
docker-compose logs -f polaris
```

**Restart a service**:
```bash
docker-compose restart jupyter
```

**Rebuild and restart (after config changes)**:
```bash
docker-compose up -d --build
```

## Troubleshooting

### Services not starting

Check if ports are already in use:
```bash
# macOS/Linux
lsof -i :8888  # Jupyter
lsof -i :8080  # Trino
lsof -i :9000  # MinIO API
lsof -i :9001  # MinIO Console
lsof -i :8181  # Polaris
```

### Check service health

```bash
# Check if containers are running
docker-compose ps

# Check specific service logs
docker-compose logs jupyter
```

### Connection refused errors

- Make sure all services are fully started (check logs)
- Services may take 1-2 minutes to initialize
- Verify network connectivity: `docker network ls`

### Spark Session: `ConnectionRefusedError: [Errno 111] Connection refused`

If you see this error when initializing a Spark Session in a notebook, the Docker setup is most likely running out of RAM and shutting down the Spark JVM. Either shut down other running notebook kernels (go to the **Running Terminals and Kernels** section in the Jupyter left sidebar) or increase the amount of RAM allocated to Docker.

### Permission errors

If you encounter permission errors with data directories:
```bash
chmod -R 755 data/
```

### Reset everything

To start fresh:
```bash
docker-compose down -v
rm -rf data/*
docker-compose up -d --build
```

## Architecture

```
┌───────────────────────────┐                  ┌───────────────────────────┐
│          Spark            │                  │    Trino (port 8080)      │
│                           │                  │                           │
│ - Spark Shell             │                  │ - SQL Query Engine        │
│ - Jupyter (port 8888)     │                  │                           │
│                           │                  │                           │
└─────────────┬─────────────┘                  └─────────────┬─────────────┘
              │                                              │
              │\                                            /│
              │ \                                          / │
              │  \          ┌─────────────────┐          /  │
              │   \         │     Polaris     │         /   │
              │    \        │   REST Catalog  │        /    │
              │     └──────▶│   (port 8181)   │◀──────┘     │
              │             └────────┬────────┘             │
              │                      │                      │
              └──────────────────────┼──────────────────────┘
                                     │
                                     ▼
              ┌───────────────────────────────────────┐
              │         MinIO (S3-compatible)         │
              │         (ports 9000/9001)             │
              │      - Bucket: warehouse              │
              │      - Parquet data files             │
              │      - Metadata files                 │
              └───────────────────────────────────────┘

All table data stored in: MinIO s3://warehouse/
All metadata managed by: Polaris REST catalog
PySpark runs in local mode (local[*]) within Jupyter container
```

## Testing Notebooks

An automated test suite is available to validate all notebooks. See [TESTING.md](TESTING.md) for instructions.

```bash
pip install -r requirements-test.txt
pytest test_notebooks.py -v
```

## Additional Resources

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Polaris Catalog Documentation](https://github.com/apache/polaris)
- [Trino Iceberg Connector](https://trino.io/docs/current/connector/iceberg.html)
- [Spark Iceberg Integration](https://iceberg.apache.org/docs/latest/spark-getting-started/)

## Notes

- All services are configured to communicate via Docker's internal network (`iceberg-net`)
- The Jupyter notebook is configured without password for ease of use (not recommended for production)
- Polaris uses a default root password `admin123` (change for production use) 
- Polaris is also set up with "in-memory" persistence, in production this should be a permanent store

## Support

If you encounter issues:
1. Check the troubleshooting section above
2. Review service logs: `docker-compose logs [service-name]`
3. Ensure your Docker has sufficient resources allocated
4. Try rebuilding: `docker-compose up -d --build`
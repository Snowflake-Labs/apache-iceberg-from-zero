# Apache Iceberg Course - Docker Setup

This Docker setup provides a complete, production-like environment for learning Apache Iceberg with:
- **MinIO**: S3-compatible object storage for table data
- **Polaris**: Apache Iceberg REST Catalog for metadata management
- **Spark**: Unified analytics engine with Iceberg support
- **Jupyter Notebook**: Interactive Python notebook with PySpark
- **Trino**: Distributed SQL query engine (for advanced querying)

All services use S3-compatible storage (MinIO) with Polaris REST catalog - a realistic production pattern.

## Version Configuration

Iceberg and related versions are centrally managed in the `.env` file:

- **Iceberg Version**: 1.10.0 (latest stable)
- **Spark Version**: 3.5 (Spark runtime version for Iceberg)
- **Scala Version**: 2.12 (required for Spark 3.5)

**Note**: We use Spark 3.5.7 with Scala 2.12 for maximum compatibility with the Jupyter PySpark notebook image. While Scala 2.13 would be preferred, Spark 3.x only supports Scala 2.12, and Spark 4.x with Scala 2.13 would require a custom Jupyter image.

To update versions, simply edit the `.env` file:

```bash
# .env
ICEBERG_VERSION=1.10.0
SPARK_VERSION=3.5
SCALA_VERSION=2.12
```

Then rebuild and restart the containers:

```bash
docker-compose up -d --build
```

## Prerequisites

- Docker Desktop installed and running
- At least 8GB of RAM allocated to Docker
- At least 10GB of free disk space

## Quick Start

1. **Navigate to the docker directory**:
   ```bash
   cd docker
   ```

2. **Start all services**:
   ```bash
   docker-compose up -d
   ```

3. **Wait for services to be ready** (approximately 1-2 minutes):
   ```bash
   docker-compose logs -f
   ```
   Press `Ctrl+C` to stop following logs once services are running.

4. **Access the services**:
   - **Jupyter Notebook**: http://localhost:8888 (no password) - **Start here!**
   - **MinIO Console**: http://localhost:9001 (admin/password) - View your data
   - **Spark Master UI**: http://localhost:8081
   - **Trino UI**: http://localhost:8080
   - **Polaris API**: http://localhost:8181

5. **Open the demo notebook**:
   - Navigate to http://localhost:8888
   - Open `iceberg_demo.ipynb`
   - Run through the cells to see Iceberg with Polaris + MinIO!

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

The Polaris catalog provides a REST API for managing Iceberg table metadata. It's configured to use MinIO for storage.

**Configuration**:
- Port: 8181
- Data directory: `./data/polaris`
- Storage: MinIO S3 (s3://warehouse/)
- OAuth2 credentials automatically generated on first start

**Initialization**:
The Polaris catalog is automatically initialized with:
- S3 storage configuration pointing to MinIO
- OAuth2 credentials (check `init-polaris.sh` output or logs)

To reinitialize:
```bash
./init-polaris.sh
```

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

### Spark

Spark is configured with Iceberg libraries and connected to the Polaris catalog.

**Configuration**:
- Master UI Port: 8081
- Master Port: 7077
- Application UI Port: 4040
- Data directory: `./data/spark`
- Config file: `./spark/conf/spark-defaults.conf`

**Run Spark Shell**:
```bash
# The Iceberg version is read from the environment variable
docker exec -it spark-master bash -c '
  /opt/spark/bin/spark-shell \
    --packages org.apache.iceberg:iceberg-spark-runtime-${SPARK_VERSION}_${SCALA_VERSION}:${ICEBERG_VERSION} \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.polaris=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.polaris.type=rest \
    --conf spark.sql.catalog.polaris.uri=http://polaris:8181/api/catalog \
    --conf spark.sql.catalog.polaris.warehouse=polaris
'

# Or with explicit version (example):
docker exec -it spark-master /opt/spark/bin/spark-shell \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.10.0 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.polaris=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.polaris.type=rest \
  --conf spark.sql.catalog.polaris.uri=http://polaris:8181/api/catalog \
  --conf spark.sql.catalog.polaris.warehouse=polaris
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

**Sample notebook**: `iceberg_demo.ipynb` is provided with examples of:
- Creating Iceberg tables
- Querying data
- ACID transactions
- Time travel
- Schema evolution
- Partitioning

## Data Persistence

All data is stored locally in the `./data` directory:
- `./data/polaris`: Polaris catalog metadata
- `./data/trino`: Trino working data
- `./data/spark`: Spark data and logs
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

**Stop and remove all data**:
```bash
docker-compose down -v
rm -rf data/*
```

**View logs**:
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f jupyter
docker-compose logs -f trino
docker-compose logs -f spark
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
lsof -i :8081  # Spark
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
                    Jupyter Notebook (port 8888)
                    - PySpark with Iceberg
                    - Python data science libraries
                              |
                              | connects to
                              ▼
                    Spark Master (ports 7077, 8081, 4040)
                    - Iceberg Spark Runtime
                    - Configured for Polaris REST catalog
                              |
                              |
        ┌─────────────────────┴─────────────────────┐
        │                                           │
        │           Polaris REST Catalog            │
        │           (port 8181)                     │
        │           - OAuth2 authentication         │
        │           - Table metadata management     │
        │           - REST API for catalogs         │
        │                                           │
        └─────────────────────┬─────────────────────┘
                              │
                              │ reads/writes data
                              ▼
                    ┌─────────────────────┐
                    │   MinIO (S3)        │
                    │   (ports 9000/9001) │
                    │   - Bucket: warehouse│
                    │   - Parquet files   │
                    │   - Metadata files  │
                    └─────────────────────┘
                              ▲
                              │ also connects
                              │
                    Trino (port 8080)
                    - Independent SQL query engine
                    - Iceberg connector
                    - Can query same tables via Polaris

Data Flow:
  1. Spark/Trino → Polaris API (get metadata)
  2. Polaris → returns table location in S3
  3. Spark/Trino → MinIO S3 (read/write table data)
  4. Spark/Trino → Polaris API (update metadata)

All table data stored in: MinIO s3://warehouse/
All metadata managed by: Polaris REST catalog
```

**Key Points**:
- **Jupyter/Spark** and **Trino** are independent query engines
- Both connect to **Polaris** for metadata (catalog API)
- **Polaris** tracks table locations, schemas, snapshots
- **MinIO** stores the actual Parquet data files
- This mirrors production architecture with AWS S3, Azure, or GCS

## Learning Path

1. **Start with Jupyter Notebook**: Open `iceberg_demo.ipynb` and run through the examples
2. **Explore with Trino**: Connect to Trino CLI and query the tables created in Jupyter
3. **Experiment with Spark**: Use Spark Shell to perform more advanced operations
4. **Deep Dive**: Explore table metadata, snapshots, and time travel features

## Additional Resources

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Polaris Catalog Documentation](https://github.com/apache/polaris)
- [Trino Iceberg Connector](https://trino.io/docs/current/connector/iceberg.html)
- [Spark Iceberg Integration](https://iceberg.apache.org/docs/latest/spark-getting-started/)

## Notes

- All services are configured to communicate via Docker's internal network (`iceberg-net`)
- The Jupyter notebook is configured without password for ease of use (not recommended for production)
- Polaris uses a default root password `admin123` (change for production use)
- Local file storage is used instead of S3/HDFS for simplicity

## Support

If you encounter issues:
1. Check the troubleshooting section above
2. Review service logs: `docker-compose logs [service-name]`
3. Ensure your Docker has sufficient resources allocated
4. Try rebuilding: `docker-compose up -d --build`


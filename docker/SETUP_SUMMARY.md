# Docker Environment - Setup Summary

## ✅ Status: Fully Operational - Production-Like Architecture

All containers are running with Polaris REST catalog and MinIO S3 storage - a realistic production pattern.

## 🚀 Services

| Service | URL | Credentials | Description |
|---------|-----|-------------|-------------|
| Jupyter Notebook | http://localhost:8888 | None | Interactive Python environment |
| MinIO Console | http://localhost:9001 | admin/password | S3-compatible storage web UI |
| MinIO API | http://localhost:9000 | admin/password | S3 API endpoint |
| Spark Master UI | http://localhost:8081 | None | Spark cluster management |
| Trino UI | http://localhost:8080 | None | SQL query engine |
| Polaris REST API | http://localhost:8181 | OAuth2 (auto-configured) | Iceberg catalog API |

## 🔧 Configuration

- **Iceberg**: v1.10.0 (latest stable release)
- **Spark**: v3.5.7 with Scala 2.12
- **Catalog Type**: Polaris REST (production pattern)
- **Storage**: MinIO S3-compatible (s3://warehouse/)
- **Authentication**: OAuth2 for Polaris

## 📝 Quick Start

```bash
# Start all services
cd docker
docker-compose up -d

# Initialize Polaris (first time only)
./init-polaris.sh

# Check status
docker-compose ps

# View logs
docker-compose logs -f jupyter

# Stop all services
docker-compose down
```

## 🧪 Verified Features

The following Iceberg features have been tested and confirmed working:

- ✅ Spark session with Iceberg extensions
- ✅ Namespace/database creation
- ✅ Table creation (regular and partitioned)
- ✅ Data insertion and querying
- ✅ ACID transactions
- ✅ Snapshot management
- ✅ Time travel queries
- ✅ Schema evolution
- ✅ Table history tracking

## 📚 Demo Notebook

Location: `./notebooks/iceberg_demo.ipynb`

The demo notebook includes comprehensive examples of:
1. Setting up Spark with Iceberg
2. Creating namespaces and tables
3. Inserting and querying data
4. Working with snapshots
5. Time travel queries
6. Schema evolution
7. Partitioned tables

## 🔄 Version Management

All versions are managed centrally in `.env`:

```bash
ICEBERG_VERSION=1.10.0
SPARK_VERSION=3.5
SCALA_VERSION=2.12
```

To upgrade, edit `.env` and run:
```bash
docker-compose down
docker-compose up -d --build
```

## 📌 Important Notes

1. **Production-Like Architecture**: This setup uses the same pattern as production deployments:
   - **Polaris REST catalog** for centralized metadata management
   - **MinIO S3 storage** for table data (same APIs as AWS S3)
   - **OAuth2 authentication** for secure catalog access
   - **Multiple query engines** (Spark & Trino) accessing the same data

2. **MinIO replaces real S3**: MinIO provides S3-compatible APIs, so:
   - Your code works identically with AWS S3, Azure Blob, or GCS
   - Great for local development and testing
   - No cloud costs during learning

3. **Automatic Initialization**: 
   - MinIO bucket (`warehouse`) created automatically
   - Polaris catalog configured with S3 storage on first run
   - OAuth2 credentials auto-generated (see `init-polaris.sh`)

4. **Data Persistence**: Data persists across container restarts in:
   - `./data/minio` - MinIO object storage (your Parquet files)
   - `./data/polaris` - Polaris metadata store
   - `./data/spark` - Spark logs and temp data

5. **View Your Data**: Use the MinIO console (http://localhost:9001) to:
   - Browse the `warehouse` bucket
   - See your Iceberg table structure
   - View Parquet files and metadata JSON files

## 🐛 Troubleshooting

### Services won't start
```bash
# Check if ports are in use
lsof -i :8888 :8080 :8081 :8181

# View logs
docker-compose logs [service-name]

# Restart fresh
docker-compose down -v
rm -rf data/* notebooks/warehouse/
docker-compose up -d --build
```

### Permission errors
```bash
chmod -R 755 data/
chmod -R 755 notebooks/
```

### Out of memory
Increase Docker's memory allocation to at least 8GB in Docker Desktop settings.

## 🎓 Next Steps

1. **Open Jupyter**: Navigate to http://localhost:8888
2. **Run Demo**: Open and execute `iceberg_demo.ipynb`
3. **Explore Features**: Try modifying the queries and creating your own tables
4. **Check UIs**: Visit the Spark and Trino UIs to see job execution

## 📖 Additional Resources

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Spark Iceberg Integration](https://iceberg.apache.org/docs/latest/spark-getting-started/)
- [Polaris Catalog](https://github.com/apache/polaris)
- [Trino Iceberg Connector](https://trino.io/docs/current/connector/iceberg.html)


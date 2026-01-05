# Use official Jupyter PySpark notebook with Spark version from .env
# See available tags: https://quay.io/repository/jupyter/pyspark-notebook?tab=tags
ARG SPARK_IMAGE_VERSION=4.0.1
FROM quay.io/jupyter/pyspark-notebook:spark-${SPARK_IMAGE_VERSION}

# Iceberg and AWS SDK versions for integration
ARG ICEBERG_VERSION=1.10.0
ARG SPARK_VERSION=4.0
ARG SCALA_VERSION=2.13
ARG AWS_SDK_VERSION=2.20.18

USER root

# Install additional dependencies
RUN apt-get update && apt-get install -y \
    vim \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Pre-download Iceberg JARs to avoid Ivy resolution issues  
# Note: Using Spark 4.0 runtime with Spark 4.1 (minor version compatibility)
RUN cd /usr/local/spark/jars && \
    wget -q "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_VERSION}_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_VERSION}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar" && \
    wget -q "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" && \
    wget -q "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_VERSION}/bundle-${AWS_SDK_VERSION}.jar" && \
    wget -q "https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/${AWS_SDK_VERSION}/url-connection-client-${AWS_SDK_VERSION}.jar"

USER ${NB_UID}

# Install Python packages
RUN pip install --no-cache-dir \
    pyiceberg[pyarrow,pandas] \
    trino \
    sqlalchemy \
    jupyterlab \
    matplotlib \
    seaborn \
    requests

WORKDIR /home/jovyan/work

CMD ["start-notebook.sh", "--NotebookApp.token=''", "--NotebookApp.password=''"]

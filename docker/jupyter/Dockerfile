FROM jupyter/pyspark-notebook:latest

USER root

# Install additional dependencies
RUN apt-get update && apt-get install -y \
    vim \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Pre-download Iceberg JARs to avoid Ivy resolution issues  
# Download them as root and put them in SPARK_HOME/jars
RUN cd /usr/local/spark/jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.0/iceberg-spark-runtime-3.5_2.12-1.10.0.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.10.0/iceberg-aws-bundle-1.10.0.jar && \
    wget -q https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.18/bundle-2.20.18.jar && \
    wget -q https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/2.20.18/url-connection-client-2.20.18.jar

USER jovyan

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

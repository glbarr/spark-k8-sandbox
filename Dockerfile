FROM apache/spark:3.5.3-python3

USER root

# Download Iceberg and Delta Lake JARs into Spark's auto-loaded jars directory
RUN curl -fsSL -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
    && curl -fsSL -o /opt/spark/jars/delta-spark_2.12-3.2.1.jar \
      https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar \
    && curl -fsSL -o /opt/spark/jars/delta-storage-3.2.1.jar \
      https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar

# Install python dependencies for spark jobs and JupyterLab
RUN pip install --no-cache-dir pyspark==3.5.3 delta-spark==3.2.1 pyiceberg==0.7.1 jupyterlab==4.2.5 ipykernel==6.29.5 pandas==2.0.3

RUN mkdir -p /opt/spark/jobs && chmod 755 /opt/spark/jobs

# Copy job files
COPY jobs/*.py /opt/spark/jobs

# Pre-configure Spark defaults (Iceberg + Delta extensions, warehouse path)
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# Copy notebooks so the Jupyter initContainer can seed them onto the PVC
COPY notebooks/ /opt/spark/notebooks/

# Set permissions
RUN chmod -R 755 /opt/spark/jobs/ /opt/spark/notebooks/ \
    && chmod 644 /opt/spark/conf/spark-defaults.conf

USER spark

WORKDIR /opt/spark/jobs
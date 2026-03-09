FROM apache/spark:3.5.3-python3

USER root

# Download Iceberg and Delta Lake JARs into Spark's auto-loaded jars directory
RUN curl -fsSL -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
    && curl -fsSL -o /opt/spark/jars/delta-spark_2.12-3.2.1.jar \
      https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar \
    && curl -fsSL -o /opt/spark/jars/delta-storage-3.2.1.jar \
      https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar

# Install python dependencies for spark jobs
RUN pip install --no-cache-dir pyspark==3.5.3 delta-spark==3.2.1 pyiceberg==0.7.1

RUN mkdir -p /opt/spark/jobs && chmod 755 /opt/spark/jobs

# Copy job files
COPY jobs/*.py /opt/spark/jobs

# Set permissions
RUN chmod -R 755 /opt/spark/jobs/

USER spark

WORKDIR /opt/spark/jobs
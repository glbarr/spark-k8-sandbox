FROM apache/spark:3.5.3-python3

USER root

# Install python dependencies for spark jobs
RUN pip install --no-cache-dir pyspark==3.5.3

RUN mkdir -p /opt/spark/jobs && chmod 755 /opt/spark/jobs

# Copy job files
COPY jobs/*.py /opt/spark/jobs

# Set permissions
RUN chmod -R 755 /opt/spark/jobs/

USER spark

WORKDIR /opt/spark/jobs
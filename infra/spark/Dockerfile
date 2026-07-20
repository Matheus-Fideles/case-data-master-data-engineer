FROM apache/spark:3.5.1-python3

USER root

# Install Python deps for pipeline code
COPY infra/spark/requirements.txt /opt/spark/work-dir/requirements.txt
RUN pip install --no-cache-dir -r /opt/spark/work-dir/requirements.txt

# Pre-downloaded JARs: Delta Lake 3.1.0 + Hadoop AWS 3.3.4 + OpenLineage 1.17.0
COPY infra/spark/jars/ /opt/spark/jars/

# Copy pipeline code into image so SparkKubernetesOperator can reference it
COPY apps/ /opt/spark/work-dir/apps/

RUN chown -R spark:spark /opt/spark/work-dir

USER spark

ENV PYTHONPATH="/opt/spark/work-dir"

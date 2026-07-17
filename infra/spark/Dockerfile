FROM apache/spark:3.5.1-python3

USER root

# Versions
ARG DELTA_VERSION=3.1.0
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.367
ARG OL_VERSION=1.17.0

# Install Python deps for pipeline code
COPY requirements.txt /opt/spark/work-dir/requirements.txt
RUN pip install --no-cache-dir -r /opt/spark/work-dir/requirements.txt

# Download Delta Lake + Hadoop AWS + OpenLineage JARs to Spark jars dir
RUN curl -fsSL \
    "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar" \
    -o /opt/spark/jars/delta-spark_2.12-${DELTA_VERSION}.jar && \
    curl -fsSL \
    "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" \
    -o /opt/spark/jars/delta-storage-${DELTA_VERSION}.jar && \
    curl -fsSL \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
    -o /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -fsSL \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
    -o /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar && \
    curl -fsSL \
    "https://repo1.maven.org/maven2/io/openlineage/openlineage-spark_2.12/${OL_VERSION}/openlineage-spark_2.12-${OL_VERSION}.jar" \
    -o /opt/spark/jars/openlineage-spark_2.12-${OL_VERSION}.jar

# Copy pipeline code into image so SparkKubernetesOperator can reference it
COPY apps/ /opt/spark/work-dir/apps/

RUN chown -R spark:spark /opt/spark/work-dir

USER spark

ENV PYTHONPATH="/opt/spark/work-dir:${PYTHONPATH}"

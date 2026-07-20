FROM apache/hive:4.0.1

USER root

ARG POSTGRES_JDBC_VERSION=42.7.3
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.367

RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

RUN curl -fsSL \
    "https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
    -o /opt/hive/lib/postgresql-${POSTGRES_JDBC_VERSION}.jar && \
    curl -fsSL \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
    -o /opt/hive/lib/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -fsSL \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
    -o /opt/hive/lib/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

COPY hive-site.xml /opt/hive/conf/hive-site.xml

USER hive

ENV SERVICE_NAME=metastore

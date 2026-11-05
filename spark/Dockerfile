FROM apache/spark:3.5.9

USER root

# Compatible Iceberg version for Java 11
ENV ICEBERG_VERSION=1.10.1

# Hadoop version included in Spark 3.5.9
ENV HADOOP_VERSION=3.3.4

# Create dependency directory
RUN mkdir -p /opt/spark/jars

# Iceberg Spark runtime
ADD https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.1/iceberg-spark-runtime-3.5_2.12-1.10.1.jar /opt/spark/jars/iceberg-spark-runtime.jar

# Iceberg AWS bundle for MinIO
ADD https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.10.1/iceberg-aws-bundle-1.10.1.jar /opt/spark/jars/iceberg-aws-bundle.jar

# Hadoop AWS connector
ADD https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/hadoop-aws.jar

# AWS SDK bundle compatible with Hadoop 3.3.4
ADD https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/aws-java-sdk-bundle.jar

# PostgreSQL JDBC driver
ADD https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.7/postgresql-42.7.7.jar /opt/spark/jars/postgresql.jar

# Spark Kafka connector
ADD https://repo.maven.apache.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.9/spark-sql-kafka-0-10_2.12-3.5.9.jar /opt/spark/jars/spark-sql-kafka.jar

ADD https://repo.maven.apache.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.9/spark-token-provider-kafka-0-10_2.12-3.5.9.jar /opt/spark/jars/spark-token-provider-kafka.jar

# Kafka client
ADD https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/kafka-clients.jar

# Commons Pool
ADD https://repo.maven.apache.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/commons-pool2.jar

# Make all downloaded JARs readable by the Spark user
RUN chmod 644 /opt/spark/jars/*.jar

USER spark
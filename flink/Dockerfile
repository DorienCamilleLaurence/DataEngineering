FROM flink:1.17.1-scala_2.12

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    pip3 install --no-cache-dir pyflink protobuf grpcio && \
    ln -s /usr/bin/python3 /usr/bin/python

# Install PyFlink
RUN pip install apache-flink==1.17.1
    
# ---------------------------------------------------------------------
# Add Kafka connector + Kafka client libs
# ---------------------------------------------------------------------

RUN curl -L -o /opt/flink/lib/flink-connector-kafka-1.17.1.jar \
        https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/1.17.1/flink-connector-kafka-1.17.1.jar && \
    curl -L -o /opt/flink/lib/kafka-clients-3.4.0.jar \
        https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar

# ---------------------------------------------------------------------
#  Add PostgreSQL JDBC driver for TimescaleDB sink
# ---------------------------------------------------------------------

RUN curl -L -o /opt/flink/lib/postgresql-42.7.3.jar \
        https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

# ---------------------------------------------------------------------
#  Add JDBC connector for Flink
# ---------------------------------------------------------------------
RUN curl -L -o /opt/flink/lib/flink-connector-jdbc-3.1.1-1.17.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.1-1.17/flink-connector-jdbc-3.1.1-1.17.jar

USER flink

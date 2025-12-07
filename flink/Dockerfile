FROM flink:1.17.1-scala_2.12

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    pip3 install --no-cache-dir pyflink && \
    ln -s /usr/bin/python3 /usr/bin/python

USER flink

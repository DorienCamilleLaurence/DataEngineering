FROM python:3.10-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends wget ca-certificates apt-utils && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingest_data.py .

ENV BROKER=redpanda:9092
ENV TOPIC=machine-sensors
ENV INTERVAL_SECONDS=2
ENV INITIAL_BACKFILL=800

ENTRYPOINT ["python", "ingest_data.py"]

FROM python:3.10-slim

COPY ./requirements.txt ./
RUN pip install -r requirements.txt
WORKDIR /app
COPY ingest_data.py ingest_data.py
ENTRYPOINT [ "python", "ingest_data.py"]
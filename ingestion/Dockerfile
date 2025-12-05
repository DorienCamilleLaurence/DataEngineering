FROM python:3.10-slim

RUN apt-get install wget
COPY ./requirements.txt . /
RUN pip install -r requirements.txt
RUN pip install psycopg2 pyarrow fastparquet
WORKDIR /app
COPY ingest_data.py ingest_data.py
ENTRYPOINT [ "python", "ingest_data.py"]
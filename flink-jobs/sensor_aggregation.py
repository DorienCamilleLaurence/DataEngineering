"""import sys
read machine-sensors

#sliding for minimum
def sliding_min(arr, k=60):
    lowest = sys.maxsize
    n = len(arr)
    for i in range(n - k + 1):
        window_min = min(arr[i:i+k])
        lowest = min(lowest, window_min)
    return lowest

# sliding for max
def sliding_max(arr, k=60):
    highest = -sys.maxsize
    n = len(arr)
    for i in range(n - k + 1):
        window_max = max(arr[i:i+k])
        highest = max(highest, window_max)
    return highest

#tumbling average
from collections import defaultdict

def tumbling_average(events, window_size=60):
    buckets = defaultdict(list)
    for ts, value in events:
        window_id = ts // window_size
        buckets[window_id].append(value)

    averages = {}
    for window_id, values in buckets.items():
        averages[window_id] = sum(values) / len(values)
    return averages



#tumbling count
def tumbling_count_average(values, window_size=60):
    averages = []
    for i in range(0, len(values), window_size):
        window = values[i:i+window_size]
        avg = sum(window) / len(window)
        averages.append(avg)
    return averages
"""

# sensor_aggregation.py
# Cleaned PyFlink job
# Added Python UDFs for sliding/tumbling logic based on user-provided functions

from pyflink.table.udf import udf
import sys
from collections import deque, defaultdict

# --- Flink Environment ---
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = TableEnvironment.create(env_settings)

# -------------------------------------------------------------------
# UDF (user defined function ): Sliding Min
# -------------------------------------------------------------------
@udf(result_type="ARRAY<DOUBLE>")
def sliding_min(arr, k: int = 60):
    if arr is None:
        return None
    dq = deque()
    mins = []
    for i, val in enumerate(arr):
        while dq and dq[0] <= i - k:
            dq.popleft()
        while dq and arr[dq[-1]] > val:     # dq[-1] = last element
            dq.pop()
        dq.append(i)
        if i >= k - 1:
            mins.append(arr[dq[0]])
    return mins

# -------------------------------------------------------------------
# UDF: Sliding Max
# -------------------------------------------------------------------
@udf(result_type="ARRAY<DOUBLE>")
def sliding_max(arr, k: int = 60):
    if arr is None:
        return None
    dq = deque()
    maxs = []
    for i, val in enumerate(arr):
        while dq and dq[0] <= i - k:
            dq.popleft()
        while dq and arr[dq[-1]] < val:
            dq.pop()
        dq.append(i)
        if i >= k - 1:
            maxs.append(arr[dq[0]])
    return maxs

# -------------------------------------------------------------------
# UDF: Tumbling Average Based on Timestamp Buckets
# events = ARRAY<ROW<ts BIGINT, value DOUBLE>>
# -------------------------------------------------------------------
@udf(result_type="MAP<BIGINT, DOUBLE>")
def tumbling_average(events, window_size: int = 60):
    if events is None:
        return None
    buckets = defaultdict(list)
    for row in events:
        ts = row[0]
        value = row[1]
        window_id = ts // window_size
        buckets[window_id].append(value)
    return {wid: sum(vals)/len(vals) for wid, vals in buckets.items()}

# -------------------------------------------------------------------
# UDF: Tumbling Count/Average by Element Count
# -------------------------------------------------------------------
@udf(result_type="ARRAY<DOUBLE>")
def tumbling_count_average(values, window_size: int = 60):
    if values is None:
        return None
    averages = []
    for i in range(0, len(values), window_size):
        window = values[i:i+window_size]
        if window:
            averages.append(sum(window) / len(window))
    return averages

# Register UDFs in Flink table env
t_env.create_temporary_system_function("sliding_min", sliding_min)
t_env.create_temporary_system_function("sliding_max", sliding_max)
t_env.create_temporary_system_function("tumbling_average", tumbling_average)
t_env.create_temporary_system_function("tumbling_count_average", tumbling_count_average)
 (no duplication with Dockerfile or ingest_data.py)
# Reads Kafka → applies tumbling & sliding windows → writes to TimescaleDB

import os
from pyflink.table import EnvironmentSettings, TableEnvironment

# Environment variables only (Dockerfile already sets Python runtime + deps)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "machine-sensors")

TIMESCALEDB_URL = os.getenv("TIMESCALEDB_URL", "jdbc:postgresql://localhost:5432/postgres")
TIMESCALEDB_USER = os.getenv("TIMESCALEDB_USER", "postgres")
TIMESCALEDB_PASS = os.getenv("TIMESCALEDB_PASS", "postgres")


# --- Kafka Source Table ---
kafka_source = f"""
CREATE TABLE machine_sensors_kafka (
    machine_id STRING,
    machine_type STRING,
    machine_model STRING,
    location STRING,
    sensor_type STRING,
    unit STRING,
    value DOUBLE,
    `timestamp` STRING,
    timestamp_ms BIGINT,
    event_time AS TO_TIMESTAMP_LTZ(timestamp_ms, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = '{KAFKA_TOPIC}',
    'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);
"""
t_env.execute_sql(kafka_source)

# --- TimescaleDB Sink Tables ---
raw_sink = f"""
CREATE TABLE raw_machine_sensors (
    machine_id STRING,
    machine_type STRING,
    machine_model STRING,
    location STRING,
    sensor_type STRING,
    unit STRING,
    value DOUBLE,
    timestamp STRING,
    timestamp_ms BIGINT
) WITH (
    'connector' = 'jdbc',
    'url' = '{TIMESCALEDB_URL}',
    'table-name' = 'machine_sensors',
    'username' = '{TIMESCALEDB_USER}',
    'password' = '{TIMESCALEDB_PASS}'
);
"""
t_env.execute_sql(raw_sink)

t_env.execute_sql(
    """
    INSERT INTO raw_machine_sensors
    SELECT machine_id, machine_type, machine_model, location, sensor_type, unit, value, `timestamp`, timestamp_ms
    FROM machine_sensors_kafka
    """
)

agg_sink = f"""
CREATE TABLE sensor_aggregates (
    machine_id STRING,
    sensor_type STRING,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    avg_value DOUBLE,
    min_value DOUBLE,
    max_value DOUBLE,
    cnt BIGINT
) WITH (
    'connector' = 'jdbc',
    'url' = '{TIMESCALEDB_URL}',
    'table-name' = 'sensor_aggregates',
    'username' = '{TIMESCALEDB_USER}',
    'password' = '{TIMESCALEDB_PASS}'
);
"""
t_env.execute_sql(agg_sink)

# --- Tumbling Window (1 minute) ---
t_env.execute_sql(
    """
    INSERT INTO sensor_aggregates
    SELECT
        machine_id,
        sensor_type,
        TUMBLE_START(event_time, INTERVAL '1' MINUTE),
        TUMBLE_END(event_time, INTERVAL '1' MINUTE),
        AVG(value),
        MIN(value),
        MAX(value),
        COUNT(*)
    FROM machine_sensors_kafka
    GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE), machine_id, sensor_type
    """
)

# --- Sliding Window (1 minute window, 30s slide) ---
t_env.execute_sql(
    """
    INSERT INTO sensor_aggregates
    SELECT
        machine_id,
        sensor_type,
        HOP_START(event_time, INTERVAL '30' SECOND, INTERVAL '1' MINUTE),
        HOP_END(event_time, INTERVAL '30' SECOND, INTERVAL '1' MINUTE),
        AVG(value),
        MIN(value),
        MAX(value),
        COUNT(*)
    FROM machine_sensors_kafka
    GROUP BY HOP(event_time, INTERVAL '30' SECOND, INTERVAL '1' MINUTE), machine_id, sensor_type
    """
)

source_table = t_env.from_path("machine_sensors_kafka")
env = StreamExecutionEnvironment.get_execution_environment()
env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

from pyflink.datastream.functions import TimestampAssigner

class MsTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, element, record_timestamp):
        # element: (machine_id, sensor_type, value, timestamp_ms)
        return element[3]

print("Sensor aggregation job started.")


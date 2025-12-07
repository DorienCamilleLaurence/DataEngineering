
import os
import json
import sys

from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.udf import udf
from collections import deque, defaultdict

from pyflink.common import WatermarkStrategy, Types, Duration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import MapFunction


from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.jdbc import JdbcSink
from pyflink.common import RuntimeExecutionMode



# -------------------------------------------------------------------
# Environment Variables
# -------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "machine-sensors")

TIMESCALE_URL = os.getenv("TIMESCALEDB_URL", "jdbc:postgresql://timescaledb:5432/iiot")
TIMESCALE_USER = os.getenv("TIMESCALEDB_USER", "admin")
TIMESCALE_PASS = os.getenv("TIMESCALEDB_PASS", "admin")

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
        while dq and arr[dq[-1]] > val:
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
# (no duplication with Dockerfile or ingest_data.py)
# Reads Kafka → applies tumbling & sliding windows → writes to TimescaleDB

print("Register Kafka source in Table API...")

# --- Kafka Source Table ---
kafka_source = f"""
CREATE TABLE machine_sensors_kafka (
    machine_id STRING,
    machine_type STRING,
    location STRING,
    sensor_type STRING,
    unit STRING,
    `value` DOUBLE,
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
    `value` DOUBLE,
    `timestamp` STRING,
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

print(" Writing RAW sensor data → TimescaleDB...")


t_env.execute_sql(
    """
    INSERT INTO raw_machine_sensors
    SELECT machine_id, machine_type, machine_model, location, sensor_type, unit, `value`, `timestamp`, timestamp_ms
    FROM machine_sensors_kafka
    """
)

# TimescaleDB sink for aggregations
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

print("Running TUMBLING WINDOW aggregation (Table API)...")

# --- Tumbling Window (1 minute) ---
t_env.execute_sql(
    """
    INSERT INTO sensor_aggregates
    SELECT
        machine_id,
        sensor_type,
        TUMBLE_START(event_time, INTERVAL '1' MINUTE),
        TUMBLE_END(event_time, INTERVAL '1' MINUTE),
        AVG(`value`),
        MIN(`value`),
        MAX(`value`),
        COUNT(*)
    FROM machine_sensors_kafka
    GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE), machine_id, sensor_type
    """
)

print("Running SLIDING WINDOW (HOP) aggregation (Table API)...")

# --- Sliding Window (1 minute window, 30s slide) ---
t_env.execute_sql(
    """
    INSERT INTO sensor_aggregates
    SELECT
        machine_id,
        sensor_type,
        HOP_START(event_time, INTERVAL '30' SECOND, INTERVAL '1' MINUTE),
        HOP_END(event_time, INTERVAL '30' SECOND, INTERVAL '1' MINUTE),
        AVG(`value`),
        MIN(`value`),
        MAX(`value`),
        COUNT(*)
    FROM machine_sensors_kafka
    GROUP BY HOP(event_time, INTERVAL '30' SECOND, INTERVAL '1' MINUTE), machine_id, sensor_type
    """
)

print("Sensor aggregation job started.")
print("Starting DataStream sliding window pipeline...")



# env = StreamExecutionEnvironment.get_execution_environment()
# env.set_parallelism(1)
# env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

# # Define Kafka Source
# #ingest data from topic KAFKA_TOPIC
# source = KafkaSource.builder() \
#     .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS) \
#     .set_topics(KAFKA_TOPIC) \
#     .set_group_id("flink-datastream-group") \
#     .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
#     .set_value_only_deserializer(SimpleStringSchema()) \
#     .build()

# #watermarks (5 seconds)
# ds = env.from_source(source, WatermarkStrategy.for_monotonous_timestamps(), "Kafka Source")

# class ParseSensorReading(MapFunction):
#     def map(self, value):
#         try:
#             d = json.loads(value)
#             ts_ms = int(datetime.fromisoformat(d["timestamp"]).timestamp() * 1000)
#             return (
#                 d["machine_id"],
#                 d["sensor_type"],
#                 d["value"],
#                 ts_ms
#             )
#         except Exception as e:
#             # eenvoudige fallback voor debugging (kan ook naar een dead-letter stream)
#             print(f"JSON parse error: {e} → value={value}")
#             return ("invalid", "invalid", 0.0, 0)
        
# parsed = ds.map(ParseSensorReading(),
#     output_type=Types.TUPLE([Types.STRING(), Types.STRING(),Types.DOUBLE(), Types.LONG()]))    

# with_timestamps = parsed.assign_timestamps_and_watermarks(
#     WatermarkStrategy
#         .for_bounded_out_of_orderness(Duration.of_seconds(5))
#         .with_timestamp_assigner(lambda e, _: e[3])
# )

# def reduce_acc(a, b):
#     return (
#         a[0], a[1],
#         a[2] + b[2],              # sum
#         min(a[3], b[3]),          # min
#         max(a[4], b[4]),          # max
#         a[5] + 1                  # count
#     )


# def finalize_result(key, window, acc):
#     avg = acc[2] / acc[5]
#     return (key[0], key[1], window.get_start(), window.get_end(), avg, acc[3], acc[4], acc[5])

# # --- Sliding Window ------------------------------------------------------
# windowed = (
#     with_timestamps
#     .key_by(lambda e: (e[0], e[1]))
#     .window(SlidingEventTimeWindows.of(Duration.of_seconds(60), Duration.of_seconds(30)))
#         .reduce(reduce_acc, finalize_result)

# )


# # -------------------------------------------------------------------
# # JDBC Sink for DataStream Aggregates
# # -------------------------------------------------------------------
# jdbc_sink = JdbcSink.sink(
#     """
#     INSERT INTO sensor_aggregates
#         (machine_id, sensor_type, window_start, window_end, avg_value, min_value, max_value, cnt)
#     VALUES (?, ?, to_timestamp(?/1000.0), to_timestamp(?/1000.0), ?, ?, ?, ?)
#     """,
#     lambda stmt, row: [
#         stmt.setString(1, row[0]),
#         stmt.setString(2, row[1]),
#         stmt.setLong(3, row[2]),
#         stmt.setLong(4, row[3]),
#         stmt.setDouble(5, row[4]),
#         stmt.setDouble(6, row[5]),
#         stmt.setDouble(7, row[6]),
#         stmt.setLong(8, row[7])
#     ],
#     Types.TUPLE([
#         Types.STRING(), Types.STRING(),
#         Types.LONG(), Types.LONG(),
#         Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.LONG()
#     ]),
#     TIMESCALE_URL,
#     TIMESCALE_USER,
#     TIMESCALE_PASS
# )

# windowed.add_sink(jdbc_sink)

# # -------------------------------------------------------------------
# # Execute DataStream job
# # -------------------------------------------------------------------

# print(" All pipelines started successfully!")
# env.execute("SlidingWindowDataStreamJob")



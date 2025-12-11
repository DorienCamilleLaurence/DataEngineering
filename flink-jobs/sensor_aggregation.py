
import os
import json
import sys
from datetime import datetime

from pyflink.table import EnvironmentSettings, TableEnvironment, Schema, DataTypes, StreamTableEnvironment

from pyflink.table.udf import udf
from collections import deque, defaultdict

from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common import Types, Duration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
# from pyflink.datastream.functions import MapFunction


# from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.common.serialization import SimpleStringSchema
# from pyflink.datastream.connectors.jdbc import JdbcSink
# from pyflink.common import RuntimeExecutionMode
from pyflink.common.watermark_strategy import TimestampAssigner

class SensorTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, event, record_timestamp):
        return int(event[5])


# ---------- Helper: JSON -> Python tuple ----------

def parse_event(value: str):
    """
    Verwacht JSON zoals:
    {
      "machine_id": "M1",
      "machine_type": "type1",
      "location": "loc1",
      "sensor_type": "temperature",
      "value": 73.2,
      "timestamp": "2025-12-07T10:23:12Z"
    }
    """
    data = json.loads(value)

    ts_str = data["timestamp"]

    if ts_str is None:
        raise ValueError("Event mist 'timestamp' veld")

    # Support zowel "...Z" als zonder 'Z'
    if ts_str.endswith("Z"):
        ts_str = ts_str.replace("Z", "+00:00")

    ts = datetime.fromisoformat(ts_str)
    print(data["machine_id"],
        data["machine_type"],
        data["location"],
        data["sensor_type"],
        float(data["value"]),
        ts)
    return (
        data["machine_id"],
        data["machine_type"],
        data["location"],
        data["sensor_type"],
        float(data["value"]),
        int(ts.timestamp() * 1000)
    )



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

# Register UDFs in Flink table env
t_env.create_temporary_system_function("sliding_min", sliding_min)
t_env.create_temporary_system_function("sliding_max", sliding_max)
t_env.create_temporary_system_function("tumbling_average", tumbling_average)
t_env.create_temporary_system_function("tumbling_count_average", tumbling_count_average)
# (no duplication with Dockerfile or ingest_data.py)
# Reads Kafka → applies tumbling & sliding windows → writes to TimescaleDB

def main():
    # ---------- Environments ----------
    # env = StreamExecutionEnvironment.get_execution_environment()
    # env.set_parallelism(1)
    # t_env = StreamTableEnvironment.create(env)

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    t_env = StreamTableEnvironment.create(env)

    # ---------- DataStream API: lees van Kafka + watermarks ----------
    # Define Kafka Source
    #ingest data from topic KAFKA_TOPIC
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS) \
        .set_topics(KAFKA_TOPIC) \
        .set_group_id("flink-sensor-consumer") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # Eerst ruwe String-messages
    raw_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "kafka-machine-sensors"
    )

    # Parse JSON -> (machine_id, sensor_type, value, ts_datetime)
    parsed_stream = raw_stream.map(
        parse_event,
        output_type=Types.TUPLE([
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.DOUBLE(),
            Types.LONG() 
        ])
    )
    # Watermarks + event-time (ts = index 3 van de tuple)
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(SensorTimestampAssigner())
    )

    timed_stream = parsed_stream.assign_timestamps_and_watermarks(
        watermark_strategy 
    )
    timed_stream.print()


# ---------- Table API: converteer DataStream -> Table met schema ----------
    events_table = t_env.from_data_stream(
        timed_stream,
        Schema.new_builder()
        .column_by_expression("machine_id", "f0")
        .column_by_expression("machine_type", "f1")
        .column_by_expression("location", "f2")
        .column_by_expression("sensor_type", "f3")
        .column_by_expression("value", "f4")
        .column_by_expression("ts", "TO_TIMESTAMP_LTZ(f5, 3)")
        .watermark("ts", "ts - INTERVAL '5' SECOND")
        .build()
    )

    # Maak een tijdelijke view voor SQL
    t_env.create_temporary_view("sensor_events", events_table)


    # Raw sensordata
    jdbc_sink = f"""
    CREATE TABLE IF NOT EXISTS machine_sensors_sink (
        machine_id STRING,
        machine_type STRING,
        location STRING,
        sensor_type STRING,
        `value` DOUBLE,
        ts TIMESTAMP(3)
    ) WITH (
        'connector' = 'jdbc',
        'url' = '{TIMESCALE_URL}',
        'table-name' = 'machine_sensors',
        'username' = '{TIMESCALE_USER}',
        'password' = '{TIMESCALE_PASS}',
        'driver' = 'org.postgresql.Driver'
    );
    """
    t_env.execute_sql(jdbc_sink)

    #Raw data -> machine_sensors
    stmt_set = t_env.create_statement_set()

    stmt_set.add_insert_sql(
        """
        INSERT INTO machine_sensors_sink
        SELECT machine_id, machine_type, location, sensor_type, `value`, CAST(ts AS TIMESTAMP(3))
        FROM sensor_events
        """
    )

    # TimescaleDB aggregations
    agg_sink = f"""
    CREATE TABLE IF NOT EXISTS sensor_aggregates (
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        machine_id STRING,
        sensor_type STRING,
        avg_value DOUBLE,
        min_value DOUBLE,
        max_value DOUBLE,
        reading_count BIGINT
    ) WITH (
        'connector' = 'jdbc',
        'url' = '{TIMESCALE_URL}',
        'table-name' = 'sensor_aggregates',
        'username' = '{TIMESCALE_USER}',
        'password' = '{TIMESCALE_PASS}'
    );
    """
    t_env.execute_sql(agg_sink)
    print("Running TUMBLING WINDOW aggregation (Table API)...")

# ---------- StatementSet: alle INSERTS in één streaming job ----------

    # --- Tumbling Window (1 minute) ---
    stmt_set.add_insert_sql(
        """
        INSERT INTO sensor_aggregates
        SELECT
            CAST(TUMBLE_START(ts, INTERVAL '1' MINUTE) AS TIMESTAMP(3)),
            CAST(TUMBLE_END(ts, INTERVAL '1' MINUTE) AS TIMESTAMP(3)),
            machine_id,
            sensor_type,
            AVG(`value`) AS avg_value,
            MIN(`value`) AS min_value,
            MAX(`value`) AS max_value,
            COUNT(*)
        FROM sensor_events
        GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE), machine_id, sensor_type
        """
    )

    print("Running SLIDING WINDOW (HOP) aggregation (Table API)...")

    # --- Sliding Window (1 minute window, 30s slide) ---
    stmt_set.add_insert_sql(
        """
        INSERT INTO sensor_aggregates
        SELECT
            CAST(HOP_START(ts, INTERVAL '30' SECOND, INTERVAL '1' MINUTE) AS TIMESTAMP(3)),
            CAST(HOP_END(ts, INTERVAL '30' SECOND, INTERVAL '1' MINUTE) AS TIMESTAMP(3)),
            machine_id,
            sensor_type,
            AVG(`value`),
            MIN(`value`),
            MAX(`value`),
            COUNT(*)
        FROM sensor_events
        GROUP BY HOP(ts, INTERVAL '30' SECOND, INTERVAL '1' MINUTE), machine_id, sensor_type
        """
    )

    print("Sensor aggregation job started.")
    print("Starting DataStream sliding window pipeline...")

    print("Registered tables:", t_env.list_tables())

    # Start de job (streaming: blijft lopen)
    result = stmt_set.execute()
    # optioneel: blokkeer zodat container niet direct stopt
    result.wait()

if __name__ == "__main__":
    main()

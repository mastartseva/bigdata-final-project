DROP TABLE IF EXISTS iot_events_kafka;

CREATE TABLE iot_events_kafka (
    device_id       STRING,
    type_id         INT,
    event_time_ms   BIGINT,
    temperature     DOUBLE,
    humidity        DOUBLE,
    event_time AS TO_TIMESTAMP_LTZ(event_time_ms, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'iot_raw_events',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-iot-sql',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);



DROP TABLE IF EXISTS iot_device_types_pg;

CREATE TABLE iot_device_types_pg (
    id        INT,
    type_name STRING
) WITH (
    'connector' = 'jdbc',
    'url'       = 'jdbc:postgresql://bd_postgres:5432/iot',
    'table-name' = 'iot_device_types',
    'username'  = 'flink',
    'password'  = 'flink',
    'driver'    = 'org.postgresql.Driver'
);

DROP TABLE IF EXISTS iot_aggregated_kafka;

CREATE TABLE iot_aggregated_kafka (
    window_time      STRING,
    type_name        STRING,
    avg_temperature  DOUBLE,
    median_humidity  DOUBLE,
    PRIMARY KEY (window_time, type_name) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'iot_aggregated',
    'properties.bootstrap.servers' = 'kafka:29092',
    'key.format' = 'json',
    'key.json.ignore-parse-errors' = 'true',
    'value.format' = 'json',
    'value.json.ignore-parse-errors' = 'true'
);

INSERT INTO iot_aggregated_kafka
SELECT
    DATE_FORMAT(window_start, 'HH:mm') AS window_time,
    dt.type_name                       AS type_name,
    AVG(T.temperature)                 AS avg_temperature,
    AVG(T.humidity)                    AS median_humidity  -- псевдо-медиана
FROM TABLE(
        TUMBLE(
            TABLE iot_events_kafka,
            DESCRIPTOR(event_time),
            INTERVAL '1' MINUTE
        )
     ) AS T
JOIN iot_device_types_pg AS dt
ON T.type_id = dt.id
GROUP BY
    window_start,
    dt.type_name;
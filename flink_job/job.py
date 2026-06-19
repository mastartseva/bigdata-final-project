import json

from pyflink.common import Types, Time
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetResetStrategy
from pyflink.datastream.functions import MapFunction

from pyflink.table import (
    StreamTableEnvironment,
    Schema,
    DataTypes
)


 # адрес из docker-compose, внутри docker-сети
KAFKA_TOPIC_IN = "iot_raw_events"
KAFKA_BOOTSTRAP = "localhost:9092"


class JsonToTuple(MapFunction):
    """
    Преобразуем JSON-строку из Kafka в Python-объект.
    Ожидаем формат, который отправляет producer/generator.py.
    """
    def map(self, value):
        obj = json.loads(value)
        return (
            str(obj["device_id"]),
            int(obj["type_id"]),
            str(obj["event_time"]),
            float(obj["temperature"]),
            float(obj["humidity"])
        )


def main():
    # 1. DataStream environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # 2. Table environment
    t_env = StreamTableEnvironment.create(env)

    # 3. Kafka source (строка)
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(KAFKA_TOPIC_IN)
        .set_group_id("flink-iot-consumer")
        .set_starting_offsets(KafkaOffsetResetStrategy.EARLIEST)
        .set_value_only_deserializer(
            # SimpleStringSchema встроен в pyflink.datastream, но проще так:
            lambda v: v.decode("utf-8")
        )
        .build()
    )

    ds_raw = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="kafka_iot_source"
    )

    # 4. Парсим JSON в кортеж
    ds_parsed = ds_raw.map(
        JsonToTuple(),
        output_type=Types.TUPLE([
            Types.STRING(),   # device_id
            Types.INT(),      # type_id
            Types.STRING(),   # event_time ISO8601
            Types.FLOAT(),    # temperature
            Types.FLOAT()     # humidity
        ])
    )

    # 5. Преобразуем в Table с явной схемой
    #
    # event_time: TIMESTAMP_LTZ(3)
    # + watermark по этому полю (позже используем как event time для окон)
    iot_schema = (
        Schema.new_builder()
        .column("device_id", DataTypes.STRING())
        .column("type_id", DataTypes.INT())
        .column("event_time_str", DataTypes.STRING())
        .column("temperature", DataTypes.FLOAT())
        .column("humidity", DataTypes.FLOAT())
        # вычисляемое поле с преобразованием строки в TIMESTAMP_LTZ
        .column_by_expression(
            "event_time",
            "TO_TIMESTAMP_LTZ(event_time_str, 3)"
        )
        .watermark("event_time", "event_time - INTERVAL '5' SECOND")
        .build()
    )

    iot_table = t_env.from_data_stream(
        ds_parsed,
        schema=iot_schema
    )

    t_env.create_temporary_view("iot_events", iot_table)

    # 6. Простой селект (пока без join/окон) для проверки
    result_table = t_env.sql_query(
        """
        SELECT
            device_id,
            type_id,
            DATE_FORMAT(event_time, 'HH:mm:ss') AS event_time_hms,
            temperature,
            humidity
        FROM iot_events
        """
    )

    # Для отладки: печать в stdout
    # (в проде лучше писать в sink)
    result_table.execute().print()


if __name__ == "__main__":
    main()
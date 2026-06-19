DROP TABLE IF EXISTS iot_device_types;

CREATE TABLE iot_device_types (
    id INTEGER PRIMARY KEY,
    type_name VARCHAR(100) NOT NULL
);
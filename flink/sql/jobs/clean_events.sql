
SET 'execution.checkpointing.interval' = '10s';
SET 'pipeline.name' = 'wikimedia-clean-events';

CREATE TABLE raw_changes (
    meta STRING,
    id STRING,
    type STRING,
    namespace STRING,
    title STRING,
    dt timestamp,
    topic STRING,
    title_url STRING,
    `comment` STRING,
    `timestamp` BIGINT,
    `user` STRING,
    bot BOOLEAN,
    notify_url STRING,
    server_url STRING,
    server_name STRING,
    server_script_path STRING,
    wiki STRING
)  WITH (
  'connector' = 'kafka',
  'topic' = 'wikimedia-recentchange',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-wikimedia',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

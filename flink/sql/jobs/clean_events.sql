
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

-- Sink: the clean topic, keyed by wiki like the raw topic.
CREATE TABLE clean_changes (
  `event_id`      STRING,
  `event_time`    TIMESTAMP_LTZ(3),
  `wiki`          STRING,
  `server_name`   STRING,
  `namespace`     STRING,
  `title`         STRING,
  `title_url`     STRING,
  `change_type`   STRING,
  `user_name`     STRING,
  `is_bot`        BOOLEAN,
  `comment`       STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'wikimedia-clean',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'raw',
  'key.fields' = 'wiki',
  'value.format' = 'json',
  'value.json.timestamp-format.standard' = 'ISO-8601'
);

-- This INSERT is what makes the file a job that keeps running.
INSERT INTO clean_changes
SELECT
  `id`,
  TO_TIMESTAMP_LTZ(`timestamp`, 0) as event_time,
  `wiki`,
  `server_name`,
  `namespace`,
  `title`,
  `title_url`,
  `type`,
  `user`,
  `bot`,
  `comment`
FROM raw_changes
WHERE `id` IS NOT NULL;
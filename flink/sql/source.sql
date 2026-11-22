-- Save Flink's position regularly and commit it to Kafka.
SET 'execution.checkpointing.interval' = '10s';
SET 'pipeline.name' = 'wikimedia-clean-events';

CREATE TABLE recent_changes (
  `wiki`      STRING,
  `title`     STRING,
  `type`      STRING,
  `bot`       BOOLEAN,
  `partition` INT    METADATA VIRTUAL,
  `offset`    BIGINT METADATA VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'wikimedia-recentchange',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-wikimedia',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);
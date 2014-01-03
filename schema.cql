CREATE KEYSPACE IF NOT EXISTS inundation WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

USE inundation;

CREATE TABLE IF NOT EXISTS queue_items (
  queue_id text,
  item_id bigint,
  item_value blob,
  PRIMARY KEY (queue_id, item_id)
);

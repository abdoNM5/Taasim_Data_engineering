CREATE KEYSPACE IF NOT EXISTS taasim WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS taasim.vehicle_positions (
  city TEXT,
  zone_id INT,
  event_time BIGINT,
  taxi_id INT,
  latitude DOUBLE,
  longitude DOUBLE,
  speed DOUBLE,
  status TEXT,
  PRIMARY KEY ((city, zone_id), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC);

CREATE TABLE IF NOT EXISTS taasim.trips (
  city TEXT,
  date_bucket TEXT,
  created_at BIGINT,
  trip_id TEXT,
  rider_id INT,
  origin_zone INT,
  destination_zone INT,
  call_type TEXT,
  PRIMARY KEY ((city, date_bucket), created_at, trip_id)
) WITH CLUSTERING ORDER BY (created_at DESC);

CREATE TABLE IF NOT EXISTS taasim.demand_zones (
  city TEXT,
  zone_id INT,
  window_start BIGINT,
  trip_count INT,
  avg_wait_time DOUBLE,
  PRIMARY KEY ((city, zone_id), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);
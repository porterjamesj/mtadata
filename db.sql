BEGIN;

CREATE EXTENSION pgcrypto;

CREATE schema mta

CREATE TABLE mta.stop_times (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    mta_id INTEGER NOT NULL,
    trip_mta_id TEXT NOT NULL,
    stop_mta_id TEXT NOT NULL,
    direction TEXT NOT NULL,
    progress NUMERIC,
    timestamp TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX no_dupliate_stop_times ON mta.stop_times (
    mta_id,
    trip_mta_id,
    stop_mta_id,
    direction,
    progress,
    timestamp
);

CREATE TABLE mta.trips (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    mta_id TEXT NOT NULL,
    route TEXT NOT NULL
);

CREATE UNIQUE INDEX no_dupliate_trips ON mta.trips (
    mta_id,
    route
);

CREATE TABLE mta.stop (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    mta_id TEXT NOT NULL,
    name TEXT NOT NULL
);

CREATE UNIQUE INDEX no_dupliate_stops ON mta.stops (
    mta_id,
    name
);


COMMIT;

CREATE OR REPLACE FUNCTION kafka_in(config json)
RETURNS TABLE(payload bytea, key bytea)
AS '$libdir/gpssfmt.so', 'gpss_worker_import'
LANGUAGE C STABLE EXECUTE ON ALL SEGMENTS;

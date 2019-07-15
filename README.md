# Standalone kafka worker, to be consumed by gpdb.

## Architecture

gpssfmt = UDF(kafka_in)
```
client(psql) ---> Master: SELECT * FROM kafka_in('...');
  /|\               |
   | grpc           |
   | Add            |
   | Start          |
   | Offset         |
   |               \|/
  \|/     grpc  / Segment (kafka_in)
worker <--------- Segment (kafka_in)
        Consume \ Segment (kafka_in)
```
1. client->worker, add kafka job
1. client->worker, start kafka job
1. client->master, SELECT * FROM kafka_in('');
1. segment->worker, consume kafka data
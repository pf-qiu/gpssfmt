Build code with docker

1. Compile protocol buffer and grpc
```
git clone -b $(curl -L https://grpc.io/release) https://github.com/grpc/grpc.git
cd /grpc
git submodule update --init --recursive
cd /grpc/third_party/protobuf/
./autogen.sh
./configure --prefix=/usr/local/grpc-dev
make -j40 install
make install
export PATH=/usr/local/grpc-dev/bin:$PATH
export LD_LIBRARY_PATH=/usr/local/grpc-dev/lib:$LD_LIBRARY_PATH

cd /grpc/
CC=gcc prefix=/usr/local/grpc-dev make -j40 install
export PKG_CONFIG_PATH=/usr/local/grpc-dev/lib/pkgconfig/
```

2. Clone rapidjson, no need to compile
```
cd /home/gpadmin
git clone https://github.com/Tencent/rapidjson.git
```

3. Compile and install extension, need to source gpdb first
```
source /home/gpadmin/greenplum-db-devel/greenplum_path.sh
make worker client all
make install
```

4. Create Kafka topic and produce some data
```
kafka-topics --zookeeper localhost --create --partitions 3 --replication-factor 1 --topic gpssfmt

rm -f /tmp/data
for ((i = 0; i < 1000000; i++))
do
echo $i, abcdefghijklmnopqrstuvwxyz >> /tmp/data
done

kafka-console-producer --broker-list 127.0.0.1:9092 --topic gpssfmt </tmp/data >/dev/null
```

5. Run
```
psql -f func.sql
./worker 127.0.0.1:8888
./client 127.0.0.1:8888 127.0.0.1:9092 gpssfmt 3
```
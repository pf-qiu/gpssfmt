#
# Copyright 2015 gRPC authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

GRPCC := $(shell pkg-config --cflags protobuf grpc)
JSONC := -I/home/gpadmin/rapidjson/include
LDFLAGS := $(shell pkg-config --libs protobuf grpc++ grpc) -ldl\
	-Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed\

MYLDFLAGS := $(LDFLAGS)
MODULE_big = gpssfmt
OBJS       = gpssfmt.o stream.grpc.pb.o stream.pb.o interface.o

SHLIB_LINK += $(shell pkg-config --libs protobuf grpc++ grpc)
PG_CPPFLAGS = -I$(libpq_srcdir) $(GRPCC) $(JSONC) -fPIC
PG_LIBS = $(libpq_pgport)

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

client: stream.pb.o stream.grpc.pb.o client.o
	$(CXX) $(CFLAGS) $^ $(MYLDFLAGS) -o $@

worker: stream.pb.o stream.grpc.pb.o worker.o
	$(CXX) $(CFLAGS) $^ $(MYLDFLAGS) -o $@ -lrdkafka


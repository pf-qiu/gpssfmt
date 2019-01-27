#include <iostream>
#include "stream.grpc.pb.h"

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <chrono>
using namespace GpssCluster;
using namespace grpc;
using namespace std;

int main(int argc, char** argv)
{
	if (argc < 2) return 1;

	auto ch = CreateChannel(argv[1], InsecureChannelCredentials());
	auto stub = KafkaWorker::NewStub(ch);
	string id;
	{
		AddRequest req;
		req.set_topic("test");
		req.set_brokers("192.168.2.2:9092");
		req.set_offset(0);
		req.set_partitionid(0);
		AddResponse res;
		ClientContext ctx;
		auto s = stub->Add(&ctx, req, &res);
		if (!s.ok())
		{
			printf("failed: %d, %s\n", s.error_code(), s.error_message().c_str());
			return 0;
		}
		id = res.id();
	}

	{
		ClientContext ctx;
		StartRequest req;
		Empty res;
		req.set_id(id);
		auto s = stub->Start(&ctx, req, &res);
		if (!s.ok())
		{
			printf("failed: %d, %s\n", s.error_code(), s.error_message().c_str());
			return 0;
		}
	}
	cout << id << endl;
}

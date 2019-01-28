#include "stream.grpc.pb.h"

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <fstream>
#include <sstream>
using namespace GpssCluster;
using namespace grpc;
using namespace std;
int main(int argc, char** argv)
{
	if (argc < 4)
	{
		printf("Usage: %s address broker topic numpart\n", argv[0]);
		return 1;
	}

	auto ch = CreateChannel(argv[1], InsecureChannelCredentials());
	auto stub = KafkaWorker::NewStub(ch);
	int partitions;
	sscanf(argv[4], "%d", &partitions);
	vector<string> ids;
	for (int i = 0; i < partitions; i++)
	{
		AddRequest req;
		req.set_topic(argv[3]);
		req.set_brokers(argv[2]);
		req.set_offset(0);
		req.set_partitionid(i);

		AddResponse res;
		ClientContext ctx;
		auto s = stub->Add(&ctx, req, &res);
		if (!s.ok())
		{
			printf("failed: %d, %s\n", s.error_code(), s.error_message().c_str());
			return 0;
		}
		ids.emplace_back(res.id());
	}

	ofstream sql("test.sql");
	const char dq = '\"';
	sql << "SELECT sum(octet_length(payload)) from kafka_in('{";
	for (const string& id : ids)
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
		sql << dq << id << dq << ':' << dq << argv[1] << dq << ',';
	}

	sql.seekp(-1, ios_base::cur);
	sql << "}');";
	sql.flush();
	sql.close();

	system("psql -f test.sql");

	for (const string& id : ids)
	{
		ClientContext ctx;
		DeleteRequest req;
		Empty res;
		req.set_id(id);
		printf("%s\n", id.c_str());
		auto s = stub->Delete(&ctx, req, &res);
		if (!s.ok())
		{
			printf("failed: %d, %s\n", s.error_code(), s.error_message().c_str());
			return 0;
		}
	}
}

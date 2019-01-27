#include <rapidjson/rapidjson.h>
#include <rapidjson/document.h>
#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include "stream.grpc.pb.h"
#include "interface.h"

using rapidjson::Document;
using rapidjson::Value;
int parse_config(const char* jsonstr, int segid, config_info* info)
{
	Document config;
	if (config.Parse(jsonstr).HasParseError())
		return 1;

	if (!config.IsObject())
		return 1;

	if (!config.HasMember("workers"))
		return 1;

	{
		const Value& v = config["workers"];
		if (!v.IsArray())
			return 1;
		const auto& arr = v.GetArray();
		if (arr.Size() == 0)
			return 1;

		int index = segid % arr.Size();
		const Value& w = arr[index];
		const char* worker = w.GetString();
		info->len_worker = strlen(worker);
	}

	{
		if (!config.HasMember("id"))
			return 1;

		const Value& v = config["id"];
		if (!v.IsString())
			return 1;

		const char* id = v.GetString();
		info->len_id = strlen(id);
	}

	return 0;
}

void fill_config(const char* jsonstr, int segid, config_data* data)
{
	Document config;
	config.Parse(jsonstr);
	const auto& workers = config["workers"].GetArray();
	const char* worker = workers[segid % workers.Size()].GetString();
	strcpy(data->worker, worker);
	const char* id = config["id"].GetString();
	strcpy(data->id, id);
}

using namespace std;
using namespace GpssCluster;
using namespace grpc;

struct worker_context {
	unique_ptr<KafkaWorker::Stub> stub;
	
	ClientContext ctx;
	int current_index;

	KafkaMessages messages;
	unique_ptr<ClientReader<KafkaMessages>> reader;
};

static string worker_error;
const char* worker_error_message()
{
	return worker_error.c_str();
}

void* new_stream_worker()
{
	return new worker_context;
}

void delete_stream_worker(void* worker)
{
	worker_context* ctx = static_cast<worker_context*>(worker);
	delete ctx;
}
int init_stream_worker(void* worker, const char* address, const char* id)
{
	worker_context* ctx = static_cast<worker_context*>(worker);
	ctx->current_index = 0;
	auto channel = CreateChannel(address, InsecureChannelCredentials());
	if (!channel)
		return 0;

	ctx->stub = KafkaWorker::NewStub(channel);
	if (!ctx->stub)
		return 0;

	ConsumeRequest req;
	req.set_id(id);
	ctx->reader = ctx->stub->Consume(&ctx->ctx, req);
	if (!ctx->reader)
		return 0;

	return 1;
}

int next_message(void* worker, stream_message* msg)
{
	worker_context* ctx = static_cast<worker_context*>(worker);
	while (ctx->current_index == ctx->messages.messages_size())
	{
		ctx->messages.clear_messages();
		ctx->current_index = 0;
		if (!ctx->reader->Read(&ctx->messages))
		{
			Status s = ctx->reader->Finish();
			if (s.ok()) //EOF
				return -1;
			else
			{
				worker_error = s.error_message();
				return s.error_code();
			}
		}
	}
	
	const auto& m = ctx->messages.messages()[ctx->current_index];
	msg->key = m.key().data();
	msg->key_length = m.key().size();
	msg->value = m.payload().data();
	msg->value_length = m.payload().size();
	ctx->current_index++;
	return 0;
}

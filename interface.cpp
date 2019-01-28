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

	const auto& obj = config.GetObject();
	if (obj.MemberCount() == 0)
		return 1;
	auto it = obj.MemberBegin() + (segid % obj.MemberCount());

	if (!it->name.IsString())
		return 1;

	if (!it->value.IsString())
		return 1;

	info->len_worker = it->value.GetStringLength();
	info->len_id = it->name.GetStringLength();
	return 0;
}

void fill_config(const char* jsonstr, int segid, config_data* data)
{
	Document config;
	config.Parse(jsonstr);
	const auto& obj = config.GetObject();
	auto it = obj.MemberBegin() + (segid % obj.MemberCount());
	strcpy(data->worker, it->value.GetString());
	strcpy(data->id, it->name.GetString());
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

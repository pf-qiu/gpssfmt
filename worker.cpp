#include <random>
#include <unordered_map>
#include <string>
#include <sstream>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <memory>
#include <chrono>
#include <librdkafka/rdkafka.h>

#include <grpc/grpc.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <grpcpp/security/server_credentials.h>

#include "stream.grpc.pb.h"

using namespace GpssCluster;
using namespace grpc;
using namespace std;

using google::protobuf::int32;
using google::protobuf::int64;

struct KafkaStream
{
	KafkaStream(string topic, int32 partition, int64 offset) :
		topic(topic),
		partition(partition),
		offset(offset),
		rk(nullptr),
		rkt(nullptr),
		last_err(RD_KAFKA_RESP_ERR_NO_ERROR),
		running(0),
		readers(0)
	{

	}
	void Init(const string& brokers)
	{
		rk = rd_kafka_new(RD_KAFKA_CONSUMER, 0, 0, 0);
		rkt = rd_kafka_topic_new(rk, topic.c_str(), 0);
		rd_kafka_brokers_add(rk, brokers.c_str());
	}
	void Deinit()
	{
		if (rkt != nullptr)
		{
			rd_kafka_topic_destroy(rkt);
			rkt = nullptr;
		}

		if (rk != nullptr)
		{
			rd_kafka_destroy(rk);
			rk = nullptr;
		}
	}

	void Start()
	{
		unique_lock<mutex> l(lock);
		if (running == 0)
		{
			running = 1;
			rd_kafka_consume_start(rkt, partition, offset);
		}
	}

	void Stop()
	{
		unique_lock<mutex> l(lock);
		if (running == 1)
		{
			running = 0;
			rd_kafka_consume_stop(rkt, partition);
		}
	}

	void WaitReaders()
	{
		unique_lock<mutex> l(lock);
		std::chrono::seconds sec(1);
		while(readers > 0)
			cv.wait_for(l, sec);
	}

	bool ReaderStart()
	{
		unique_lock<mutex> l(lock);
		if (running != 0)
		{
			readers++;
			return true;
		}
		else
		{
			return false;
		}
	}

	void ReaderFinish(int64 messages)
	{
		unique_lock<mutex> l(lock);
		readers--;

		if (readers == 0)
		{
			cv.notify_all();
		}
	}

	string topic;
	int32 partition;
	int64 offset;

	rd_kafka_t* rk;
	rd_kafka_topic_t* rkt;
	rd_kafka_resp_err_t last_err;

	mutex lock;
	int running;
	int readers;
	condition_variable cv;
};

struct ConsumeContext
{
	ConsumeContext() :
		count(0),
		last_err(RD_KAFKA_RESP_ERR_NO_ERROR),
		writer(nullptr)
	{
	}
	KafkaMessages messages;
	int64 count;
	rd_kafka_resp_err_t last_err;
	ServerWriter<KafkaMessages>* writer;
};

class KafkaStreamWorker : public KafkaWorker::Service
{
	typedef shared_ptr<KafkaStream> MyStream;
public:
	KafkaStreamWorker() : seed(random_device()()) {}
	virtual ~KafkaStreamWorker() {}
	virtual Status Add(ServerContext* context, const AddRequest* request, AddResponse* response)
	{
		string key = GetUniqueID(request->topic(), request->partitionid());
		printf("New stream %s\n", key.c_str());
		unique_lock<mutex> l(m);

		if (streams.find(key) == streams.end())
		{
			MyStream ms = make_shared<KafkaStream>(request->topic(), request->partitionid(), request->offset());
			ms->Init(request->brokers());
			streams[key] = ms;
		}

		response->set_id(key);
		return Status::OK;
	}
	virtual Status Start(ServerContext* context, const StartRequest* request, Empty* response)
	{
		return FindAndExecute<StartRequest, Empty>(request, response, &KafkaStreamWorker::StartInternal);
	}
	Status StartInternal(MyStream ms, const StartRequest*, Empty*)
	{
		ms->Start();
		return Status::OK;
	}
	virtual Status Stop(ServerContext* context, const StopRequest* request, Empty* response)
	{
		return FindAndExecute<StopRequest, Empty>(request, response, &KafkaStreamWorker::StopInternal);
	}
	Status StopInternal(MyStream ms, const StopRequest*, Empty*)
	{
		ms->Stop();
		return Status::OK;
	}
	virtual Status Delete(ServerContext* context, const DeleteRequest* request, Empty* response)
	{
		return FindAndExecute<DeleteRequest, Empty>(request, response, &KafkaStreamWorker::DeleteInternal);
	}
	Status DeleteInternal(MyStream ms, const DeleteRequest* req, Empty*)
	{
		ms->Stop();
		ms->WaitReaders();
		streams.erase(req->id());
		return Status::OK;
	}
	virtual Status Consume(ServerContext* context, const ConsumeRequest* request, ServerWriter<KafkaMessages>* writer)
	{
		printf("Consume: %s\n", request->id().c_str());
		return FindAndExecute<ConsumeRequest, ServerWriter<KafkaMessages>, true>(request, writer, &KafkaStreamWorker::ConsumeInternal);
	}
	Status ConsumeInternal(MyStream ms, const ConsumeRequest* request, ServerWriter<KafkaMessages>* writer)
	{
		if (!ms->ReaderStart())
		{
			// Stream already stopped.
			return Status::OK;
		}

		ConsumeContext ctx;
		ctx.writer = writer;
		while (ctx.last_err == 0 && ms->last_err == 0)
		{
			rd_kafka_consume_callback(ms->rkt, ms->partition, 100, [](rd_kafka_message_t* msg, void* p) {
				auto ctx = static_cast<ConsumeContext*>(p);
				if (msg->err != 0)
				{
					printf("set err: %d\n", msg->err);
					ctx->last_err = msg->err;
				}
				else
				{
					KafkaMessage* km = ctx->messages.add_messages();
					km->set_key(msg->key, msg->key_len);
					km->set_payload(msg->payload, msg->len);
					if (ctx->messages.messages_size() >= 1000)
					{
						ctx->writer->Write(ctx->messages);
						ctx->count += ctx->messages.messages_size();
						ctx->messages.clear_messages();
					}
				}
			}, &ctx);
			ctx.count += ctx.messages.messages_size();
			writer->Write(ctx.messages);
			ctx.messages.clear_messages();
		}
		printf("Finished at offset %ld\n", ms->offset);
		if (ctx.last_err != 0)
			ms->last_err = ctx.last_err;

		if (ctx.last_err == 0 || ctx.last_err == RD_KAFKA_RESP_ERR__PARTITION_EOF)
		{
			ms->ReaderFinish(ctx.count);
			return Status::OK;
		}
		else
		{
			return Status(StatusCode::INTERNAL, rd_kafka_err2str(ctx.last_err));
		}
	}
	virtual Status Offset(ServerContext* context, const OffsetRequest* request, OffsetResponse* response)
	{
		return FindAndExecute<OffsetRequest, OffsetResponse>(request, response, &KafkaStreamWorker::OffsetInternal);
	}
	Status OffsetInternal(MyStream ms, const OffsetRequest*, OffsetResponse* res)
	{
		res->set_offset(ms->offset);
		return Status::OK;
	}
private:
	template<typename Req, typename Resp, bool NoLock = false>
	Status FindAndExecute(const Req* req, Resp* resp, Status(KafkaStreamWorker::*fun)(MyStream, const Req*, Resp*))
	{
		unique_lock<mutex> l(m);
		auto it = streams.find(req->id());
		if (it != streams.end())
		{
			if (NoLock)
			{
				l.release()->unlock();
			}
			return (this->*fun)(it->second, req, resp);
		}
		else
		{
			return Status(StatusCode::NOT_FOUND, "Stream not found");
		}
	}

	string GetUniqueID(const string& topic, int32 partition)
	{
		stringstream ss;
		ss << topic;
		ss << partition;
		ss << seed;
		return ss.str();
	}

	unordered_map<string, MyStream> streams;
	unsigned int seed;
	mutex m;
};

int main(int argc, char** argv)
{
	if (argc < 2)
	{
		printf("Usage: %s listen_address\n", argv[0]);
		return 1;
	}
	KafkaStreamWorker worker;
	
	ServerBuilder builder;
	int port = 0;
	builder.AddListeningPort(argv[1], InsecureServerCredentials(), &port);
	builder.RegisterService(&worker);
	auto s = builder.BuildAndStart();
	if (!s) return 1;

	s->Wait();
	return 0;
}

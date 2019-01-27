#ifndef INTERFACE_H
#define INTERFACE_H

#ifdef __cplusplus
extern "C" {
#endif
typedef struct {
	int len_worker;
	int len_id;
} config_info;

typedef struct {
	char* worker;
	char* id;
} config_data;

int parse_config(const char* jsonstr, int segid, config_info* info);
void fill_config(const char* jsonstr, int segid, config_data* data);

typedef struct {
	const char* key;
	int key_length;

	const char* value;
	int value_length;
} stream_message;

void* new_stream_worker();
void delete_stream_worker(void*);
int init_stream_worker(void* worker, const char* address, const char* id);
int next_message(void* worker, stream_message* msg);
const char* worker_error_message();
#ifdef __cplusplus
}
#endif

#endif

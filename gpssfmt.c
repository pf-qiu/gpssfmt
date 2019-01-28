#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"

#include "access/formatter.h"
#include "catalog/pg_proc.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/typcache.h"
#include "utils/syscache.h"
#include "utils/datetime.h"
#include "utils/jsonapi.h"
#include "cdb/cdbvars.h"
#include "utils/elog.h"
#include "interface.h"

/* Do the module magic dance */
PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(gpss_worker_import);

Datum gpss_worker_import(PG_FUNCTION_ARGS);

Datum gpss_worker_import(PG_FUNCTION_ARGS)
{
	void* worker;
	FuncCallContext *funcctx;
	int segid = GpIdentity.segindex;
	if (segid < 0) segid = 0;
	if (SRF_IS_FIRSTCALL())
	{
		text* arg = PG_GETARG_TEXT_P(0);
		char* json = text_to_cstring(arg);
		config_info info;
		if (parse_config(json, segid, &info))
			ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("invalid config")));
		
		config_data data;
		data.worker = palloc(info.len_worker + 1);
		data.id = palloc(info.len_id + 1);
		fill_config(json, segid, &data);
		elog(INFO, "worker: %s, id %s", data.worker, data.id);
		
		funcctx = SRF_FIRSTCALL_INIT();
		worker = new_stream_worker();
		if (init_stream_worker(worker, data.worker, data.id))
		{
			funcctx->user_fctx = worker;
		}
		else
		{
			delete_stream_worker(worker);
			elog(ERROR, "failed to init worker context");
		}
		
	}

	funcctx = SRF_PERCALL_SETUP();
	worker = funcctx->user_fctx;
	Oid resultid;
	TupleDesc desc;
	if (get_call_result_type(fcinfo, &resultid, &desc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type mismatch");

	if (desc == NULL || desc->natts != 2)
		 elog(ERROR, "return type mismatch");

	if (desc->natts != 2)
	{
		elog(ERROR, "only support key, value columns");
	}

	stream_message msg;
	int code = next_message(worker, &msg);
	if (code == -1)
	{
		delete_stream_worker(worker);
		elog(INFO, "EOF %d", segid);
		SRF_RETURN_DONE(funcctx);
	}
	if (code != 0)
	{
		delete_stream_worker(worker);
		elog(ERROR, "worker failed: %d, %s", code, worker_error_message());
	}

	Datum values[2];
	char nulls[2];

	if (msg.value_length > 0)
	{
		bytea *value = (bytea*)palloc(msg.value_length + VARHDRSZ);
		SET_VARSIZE(value, msg.value_length + VARHDRSZ);
		memcpy(VARDATA(value), msg.value, msg.value_length);
		values[0] = PointerGetDatum(value);
		nulls[0] = ' ';
	}
	else
	{
		values[0] = 0;
		nulls[0] = 'n';
	}

	if (msg.key_length > 0)
	{
		bytea *key = (bytea*)palloc(msg.key_length + VARHDRSZ);
		SET_VARSIZE(key, msg.key_length + VARHDRSZ);
		memcpy(VARDATA(key), msg.key, msg.key_length);
		values[1] = PointerGetDatum(key);
		nulls[1] = ' ';
	}
	else
	{
		values[1] = 0;
		nulls[1] = 'n';
	}
	HeapTuple tuple = heap_formtuple(desc, values, nulls);
	SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
}


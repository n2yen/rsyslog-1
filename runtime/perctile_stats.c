/*
 * This file is part of the rsyslog runtime library.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *       -or-
 *       see COPYING.ASL20 in the source distribution
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "config.h"
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <assert.h>

#include "unicode-helper.h"
#include "rsyslog.h"
#include "rsconf.h"
#include "errmsg.h"
#include "perctile_stats.h"
#include "hashtable_itr.h"

#include <stdio.h>
#include <assert.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <math.h>

//#define PERCTILE_STATS_DEBUG
#ifdef PERCTILE_STATS_DEBUG
#define _DEBUG 1
#else
#define _DEBUG 0
#endif
#define PERCTILE_STATS_LOG(...) do { if(_DEBUG) fprintf(stderr, __VA_ARGS__); } while(0)

/* definitions for objects we access */
DEFobjStaticHelpers
DEFobjCurrIf(statsobj)

#define PERCTILE_PARAM_NAME        "name"
#define PERCTILE_PARAM_PERCENTILES "percentiles"
#define PERCTILE_PARAM_WINDOW_SIZE "windowsize"

#define PERCTILE_MAX_BUCKET_NS_METRIC_LENGTH  128
#define PERCTILE_METRIC_NAME_SEPARATOR        '.'

static struct cnfparamdescr modpdescr[] = {
	{ PERCTILE_PARAM_NAME, eCmdHdlrString, CNFPARAM_REQUIRED },
	{ PERCTILE_PARAM_PERCENTILES, eCmdHdlrArray, 0},
	{ PERCTILE_PARAM_WINDOW_SIZE, eCmdHdlrPositiveInt, 0},
};

static struct cnfparamblk modpblk = {
	CNFPARAMBLK_VERSION,
	sizeof(modpdescr)/sizeof(struct cnfparamdescr),
	modpdescr
};

rsRetVal
perctileClassInit(void) {
	DEFiRet;
	CHKiRet(objGetObjInterface(&obj));
	CHKiRet(objUse(statsobj, CORE_COMPONENT));
finalize_it:
	RETiRet;
}

static uint64_t min(uint64_t a, uint64_t b) {
	return a < b ? a : b;
}

static uint64_t max(uint64_t a, uint64_t b) {
	return a > b ? a : b;
}

/*
 * circ buf macros derived from linux/circ_buf.h
 */

typedef int64_t ITEM;

struct circ_buf {
	ITEM *buf;
	int head;
	int tail;
};

/* Return count in buffer.  */
#define CIRC_CNT(head,tail,size) (((head) - (tail)) & ((size)-1))

/* Return space available, 0..size-1.  We always leave one free char
	 as a completely full buffer has head == tail, which is the same as
	 empty.  */
#define CIRC_SPACE(head,tail,size) CIRC_CNT((tail),((head)+1),(size))

/* Return count up to the end of the buffer.  Carefully avoid
	 accessing head and tail more than once, so they can change
	 underneath us without returning inconsistent results.  */
#define CIRC_CNT_TO_END(head,tail,size) \
	({int end = (size) - (tail); \
	 int n = ((head) + end) & ((size)-1); \
	 n < end ? n : end;})

/* Return space available up to the end of the buffer.  */
#define CIRC_SPACE_TO_END(head,tail,size) \
	({int end = (size) - 1 - (head); \
	 int n = (end + (tail)) & ((size)-1); \
	 n <= end ? n : end+1;})

/* Move head by size. */
#define CIRC_ADD(idx, size, offset)	(((idx) + (offset)) & ((size) - 1))

// simple use of the linux defined circular buffer.

typedef struct ringbuf_s {
	struct circ_buf cb;
	size_t size;
} ringbuf_t;

ringbuf_t* ringbuf_new(size_t count) {
	// use nearest power of 2
	double x = ceil(log2(count));
	size_t bufsize = pow(2, x);

	ringbuf_t *rb = calloc(1, sizeof(ringbuf_t));
	// note count needs to be a power of 2, otherwise our macros won't work.
	ITEM *pbuf = calloc(bufsize, sizeof(ITEM));
	rb->cb.buf = pbuf;
	rb->cb.head = rb->cb.tail = 0;
	rb->size = bufsize;

	return rb;
}

void ringbuf_del(ringbuf_t *rb) {
	if (rb) {
		if (rb->cb.buf) {
			free(rb->cb.buf);
		}
		free(rb);
	}
}

int ringbuf_append(ringbuf_t *rb, ITEM item) {
	// lock it and add
	int head = rb->cb.head,
			tail = rb->cb.tail;

	if (!CIRC_SPACE(head, tail, rb->size)) {
		return -1;
	} else {
		/* insert item into buffer */
		rb->cb.buf[head] = item;
		// move head
		rb->cb.head = CIRC_ADD(head, rb->size, 1);
	}
	return 0;
}

int ringbuf_append_with_overwrite(ringbuf_t *rb, ITEM item) {
	int head = rb->cb.head,
			tail = rb->cb.tail;

	if (!CIRC_SPACE(head, tail, rb->size)) {
		rb->cb.tail = CIRC_ADD(tail, rb->size, 1);
	}
	int ret = ringbuf_append(rb, item);
	assert(ret == 0); // we shouldn't fail due to no space.
	return ret;
}

int ringbuf_read(ringbuf_t *rb, ITEM *buf, size_t count) {
	int head = rb->cb.head,
			tail = rb->cb.tail;

	if (!CIRC_CNT(head, tail, rb->size)) {
		return 0;
	}

	// copy to end of buffer
	size_t copy_size = min((size_t)CIRC_CNT_TO_END(head, tail, rb->size), count);
	memcpy(buf, rb->cb.buf+tail, copy_size*sizeof(ITEM));

	rb->cb.tail = CIRC_ADD(rb->cb.tail, rb->size, copy_size);
	return copy_size;
}

size_t ringbuf_read_to_end(ringbuf_t *rb, ITEM *buf, size_t count) {
	size_t nread = 0;
	nread += ringbuf_read(rb, buf, count);
	if (nread == 0) {
		return nread;
	}
	// read the rest if buf circled around
	nread += ringbuf_read(rb, buf+nread, count);
	return nread;
}

bool ringbuf_peek(ringbuf_t *rb, ITEM *item) {
	if (CIRC_CNT(rb->cb.head, rb->cb.tail, rb->size) == 0) {
		return false;
	}

	*item = rb->cb.buf[rb->cb.head];
	return true;
}

static void perctileStatDestruct(void* p) {
	if (p) {
		perctile_stat_t *perc_stat = (perctile_stat_t *)p;
		if (perc_stat->name) {
			free(perc_stat->name);
		}
		if (perc_stat->rb_observed_stats) {
			ringbuf_del(perc_stat->rb_observed_stats);
		}
		if (perc_stat->ctrs) {
			for (size_t i = 0; i < perc_stat->perctile_ctrs_count; ++i) {
				free(perc_stat->ctrs[i].name);
			}
			free(perc_stat->ctrs);
		}
		pthread_rwlock_destroy(&perc_stat->stats_lock);
		//pthread_rwlock_destroy(&perc_stat->rb_lock);
		free(perc_stat);
	}
}

static void perctileBucketDestruct(perctile_bucket_t *bkt) {
	PERCTILE_STATS_LOG("destructing perctile bucket\n");
	if (bkt) {
		pthread_rwlock_wrlock(&bkt->lock);
		hashtable_destroy(bkt->htable, 1); // destroy all perctile stats
		statsobj.Destruct(&bkt->statsobj);
		pthread_rwlock_unlock(&bkt->lock);
		pthread_rwlock_destroy(&bkt->lock);
		free(bkt->perctile_values);
		free(bkt->name);
		free(bkt);
	}
}

void perctileBucketsDestruct() {
	perctile_buckets_t *bkts = &loadConf->perctile_buckets;

	if (bkts->initialized) {
		perctile_bucket_t *head = bkts->listBuckets;
		if (head) {
			pthread_rwlock_wrlock(&bkts->lock);
			perctile_bucket_t *pnode = head, *pnext = NULL;
			while (pnode) {
				pnext = pnode->next;
				perctileBucketDestruct(pnode);
				pnode = pnext;
			}
			pthread_rwlock_unlock(&bkts->lock);
		}
		statsobj.Destruct(&bkts->global_stats);
		// destroy any global stats we keep specifically for this.
		pthread_rwlock_destroy(&bkts->lock);
	}
}

static perctile_bucket_t*
findBucket(perctile_bucket_t *head, const uchar *name) {
	perctile_bucket_t *pbkt_found = NULL;
	// walk the linked list until the name is found
	pthread_rwlock_rdlock(&head->lock);
	for (perctile_bucket_t *pnode = head; pnode != NULL; pnode = pnode->next) {
		if (ustrcmp(name, pnode->name) == 0) {
			// found.
			pbkt_found = pnode;
		}
	}
	pthread_rwlock_unlock(&head->lock);
	return pbkt_found;
}

#ifdef PERCTILE_STATS_DEBUG
static void
print_perctiles(perctile_bucket_t *bkt) {
	if (hashtable_count(bkt->htable)) {
		struct hashtable_itr *itr = hashtable_iterator(bkt->htable);
		do {
			uchar* key = hashtable_iterator_key(itr);
			perctile_stat_t *perc_stat = hashtable_iterator_value(itr);
			PERCTILE_STATS_LOG("print_perctile() - key: %s, perctile stat name: %s ", key, perc_stat->name);
		} while (hashtable_iterator_advance(itr));
		PERCTILE_STATS_LOG("\n");
	}
}
#endif

// Assumes a fully created pstat and bkt, also initiliazes some values in pstat.
static rsRetVal
initAndAddPerctileMetrics(perctile_bucket_t *bkt, perctile_stat_t *pstat) {
	char stat_name[128];
	DEFiRet;

	size_t offset = snprintf(stat_name, sizeof(stat_name), "%s_%s_", (char*)bkt->name, (char*)pstat->name);
	size_t remaining_size = sizeof(stat_name) - offset;

	// initialize the counters array
	for (size_t i = 0; i < pstat->perctile_ctrs_count; ++i) {
		perctile_ctr_t *ctr = &pstat->ctrs[i];

		// bucket contains the supported percentile values.
		ctr->percentile = bkt->perctile_values[i];
		snprintf(stat_name+offset, remaining_size, "p%d", bkt->perctile_values[i]);
		CHKmalloc(ctr->name = ustrdup(stat_name));

		PERCTILE_STATS_LOG("perctile_observe - creating perctile stat counter: %s\n",
				ctr->name);
		CHKiRet(statsobj.AddCounter(bkt->statsobj, (uchar *)ctr->name, ctrType_IntCtr, CTR_FLAG_NONE, &ctr->perctile_stat));
	}

	strncpy(stat_name+offset, "window_min", remaining_size);
	CHKiRet(statsobj.AddCounter(bkt->statsobj, (uchar *)stat_name, ctrType_IntCtr, CTR_FLAG_NONE, &pstat->ctrWindowMin));

	strncpy(stat_name+offset, "window_max", remaining_size);
	CHKiRet(statsobj.AddCounter(bkt->statsobj, (uchar *)stat_name, ctrType_IntCtr, CTR_FLAG_NONE, &pstat->ctrWindowMax));

	strncpy(stat_name+offset, "window_sum", remaining_size);
	CHKiRet(statsobj.AddCounter(bkt->statsobj, (uchar *)stat_name, ctrType_IntCtr, CTR_FLAG_NONE, &pstat->ctrWindowSum));

	strncpy(stat_name+offset, "window_count", remaining_size);
	CHKiRet(statsobj.AddCounter(bkt->statsobj, (uchar *)stat_name, ctrType_IntCtr, CTR_FLAG_NONE, &pstat->ctrWindowCount));


	// historical counters
	strncpy(stat_name+offset, "historical_window_min", remaining_size);
	CHKiRet(statsobj.AddCounter(bkt->statsobj, (uchar *)stat_name, ctrType_IntCtr, CTR_FLAG_NONE, &pstat->ctrHistoricalWindowMin));

	strncpy(stat_name+offset, "historical_window_max", remaining_size);
	CHKiRet(statsobj.AddCounter(bkt->statsobj, (uchar *)stat_name, ctrType_IntCtr, CTR_FLAG_NONE, &pstat->ctrHistoricalWindowMax));

	strncpy(stat_name+offset, "historical_window_sum", remaining_size);
	CHKiRet(statsobj.AddCounter(bkt->statsobj, (uchar *)stat_name, ctrType_IntCtr, CTR_FLAG_NONE, &pstat->ctrHistoricalWindowSum));

	strncpy(stat_name+offset, "historical_window_count", remaining_size);
	CHKiRet(statsobj.AddCounter(bkt->statsobj, (uchar *)stat_name, ctrType_IntCtr, CTR_FLAG_NONE, &pstat->ctrHistoricalWindowCount));

finalize_it:
	RETiRet;
}

static rsRetVal
perctile_observe(perctile_bucket_t *bkt, uchar* key, int64_t value) {
	uint8_t lock_initialized = 0;
	uchar* hash_key = NULL;
	DEFiRet;

	pthread_rwlock_wrlock(&bkt->lock);
	lock_initialized = 1;
	perctile_stat_t *pstat = (perctile_stat_t*) hashtable_search(bkt->htable, key);
	if (!pstat) {
		PERCTILE_STATS_LOG("perctile_observe(): key '%s' not found - creating new pstat", key);
		// create the pstat if not found
		CHKmalloc(pstat = calloc(1, sizeof(perctile_stat_t)));
		CHKmalloc(pstat->name = ustrdup(key));
		CHKmalloc(pstat->ctrs = (perctile_ctr_t*)calloc(bkt->perctile_values_count, sizeof(perctile_stat_t)));
		pstat->perctile_ctrs_count = bkt->perctile_values_count;
		CHKmalloc(pstat->rb_observed_stats = ringbuf_new(bkt->window_size));
		//pthread_rwlock_init(&pstat->rb_lock, NULL);
		pthread_rwlock_init(&pstat->stats_lock, NULL);

		// init all stat counters here
		pthread_rwlock_wrlock(&pstat->stats_lock);
		pstat->ctrWindowCount = pstat->ctrWindowMax = pstat->ctrWindowSum = 0;
		pstat->ctrWindowMin = sizeof(pstat->ctrWindowMin);
		pstat->ctrHistoricalWindowCount = pstat->ctrHistoricalWindowMax =  pstat->ctrHistoricalWindowSum = 0;
		pstat->ctrHistoricalWindowMin = sizeof(pstat->ctrHistoricalWindowMin);
		pthread_rwlock_unlock(&pstat->stats_lock);

		CHKiRet(initAndAddPerctileMetrics(bkt, pstat));
		CHKmalloc(hash_key = ustrdup(key));
		if (!hashtable_insert(bkt->htable, hash_key, pstat)) {
			perctileStatDestruct(pstat);
			free(hash_key);
			ABORT_FINALIZE(RS_RET_OUT_OF_MEMORY);
		}
		PERCTILE_STATS_LOG("perctile_observe - new pstat created - name: %s\n", pstat->name);
		STATSCOUNTER_INC(bkt->ctrNewKeyAdd, b->mutCtrNewKeyAdd);
	}

	// add this value into the ringbuffer
	assert(pstat->rb_observed_stats);
	if (ringbuf_append_with_overwrite(pstat->rb_observed_stats, value) != 0) {
		// ringbuffer is operating in overwrite mode, so should never see this.
		ABORT_FINALIZE(RS_RET_ERR);
	}

	// update perctile specific stats
	pthread_rwlock_wrlock(&pstat->stats_lock);
	{
		(pstat->ctrWindowCount)++;
		pstat->ctrWindowSum += value;
		assert(value != 0);
		assert(pstat->ctrWindowMin != 0);
		pstat->ctrWindowMin = min(pstat->ctrWindowMin, value);
		assert(pstat->ctrWindowMin != 0);
		pstat->ctrWindowMax = max(pstat->ctrWindowMax, value);

		(pstat->ctrHistoricalWindowCount)++;
		pstat->ctrHistoricalWindowSum += value;
		pstat->ctrHistoricalWindowMin = min(pstat->ctrHistoricalWindowMin, value);
		pstat->ctrHistoricalWindowMax = max(pstat->ctrHistoricalWindowMax, value);
	}
	pthread_rwlock_unlock(&pstat->stats_lock);

#if PERCTILE_STATS_DEBUG
	PERCTILE_STATS_LOG("perctile_observe - appended value: %lld to ringbuffer\n", value);
	PERCTILE_STATS_LOG("ringbuffer contents... \n");
	for (size_t i = 0; i < pstat->rb_observed_stats->size; ++i) {
		PERCTILE_STATS_LOG("%lld ", pstat->rb_observed_stats->cb.buf[i]);
	}
	PERCTILE_STATS_LOG("\n");
	print_perctiles(bkt);
#endif
finalize_it:
	if (lock_initialized) {
		pthread_rwlock_unlock(&bkt->lock);
	}
	if (iRet != RS_RET_OK) {
		// clean up if there was an error
		if (iRet == RS_RET_OUT_OF_MEMORY) {
			STATSCOUNTER_INC(bkt->ctrOpsOverflow, bkt->mutCtrOpsOverflow);
		}
	}
	RETiRet;
}

static int cmp(const void* p1, const void* p2) {
	return (*(ITEM*)p1) - (*(ITEM*)p2);
}

static void report_perctile_stats(perctile_bucket_t* pbkt) {
	ITEM *buf = NULL;
	struct hashtable_itr *itr = NULL;
	DEFiRet;

	pthread_rwlock_rdlock(&pbkt->lock);
	if (hashtable_count(pbkt->htable)) {
		itr = hashtable_iterator(pbkt->htable);
		CHKmalloc(buf = malloc(pbkt->window_size*sizeof(ITEM)));
		do {
			memset(buf, 0, pbkt->window_size*sizeof(ITEM));
			perctile_stat_t *perc_stat = hashtable_iterator_value(itr);
			// ringbuffer read
			size_t count = ringbuf_read_to_end(perc_stat->rb_observed_stats, buf, pbkt->window_size);
			perc_stat->ctrWindowCount = 0;
			if (!count) {
				FINALIZE;
			}
			PERCTILE_STATS_LOG("read %zu values\n", count);
			// calculate the p95 based on the 
			PERCTILE_STATS_LOG("ringbuffer contents... \n");
			for (size_t i = 0; i < perc_stat->rb_observed_stats->size; ++i) {
				PERCTILE_STATS_LOG("%lld ", perc_stat->rb_observed_stats->cb.buf[i]);
			}
			PERCTILE_STATS_LOG("\n");

			PERCTILE_STATS_LOG("buffer contents... \n");
			for (size_t i = 0; i < perc_stat->rb_observed_stats->size; ++i) {
				PERCTILE_STATS_LOG("%lld ", buf[i]);
			}
			PERCTILE_STATS_LOG("\n");
			qsort(buf, count, sizeof(ITEM), cmp);

			PERCTILE_STATS_LOG("buffer contents after sort... \n");
			for (size_t i = 0; i < perc_stat->rb_observed_stats->size; ++i) {
				PERCTILE_STATS_LOG("%lld ", buf[i]);
			}
			PERCTILE_STATS_LOG("\n");

			PERCTILE_STATS_LOG("report_perctile_stats() - perctile stat has %zu counters.", perc_stat->perctile_ctrs_count);
			for (size_t i = 0; i < perc_stat->perctile_ctrs_count; ++i) {
				perctile_ctr_t *pctr = &perc_stat->ctrs[i];
				// get percentile - this can be cached.
				int index = ((pctr->percentile/100.0) * count)-1;
				// look into if we need to lock this.
				pctr->perctile_stat = buf[index];
				PERCTILE_STATS_LOG("report_perctile_stats() - index: %d, perctile stat [%s, %d, %llu]", index, pctr->name, pctr->percentile, pctr->perctile_stat);
			}
		} while (hashtable_iterator_advance(itr));
	}

finalize_it:
	pthread_rwlock_unlock(&pbkt->lock);
	free(itr);
	free(buf);
}

static void
perctile_readCallback(statsobj_t __attribute__((unused)) *ignore, void __attribute__((unused)) *b) {
	perctile_buckets_t *bkts = &loadConf->perctile_buckets;

	pthread_rwlock_rdlock(&bkts->lock);
	for (perctile_bucket_t *pbkt = bkts->listBuckets; pbkt != NULL; pbkt = pbkt->next) {
		report_perctile_stats(pbkt);
	}
	pthread_rwlock_unlock(&bkts->lock);
}

static rsRetVal
perctileInitNewBucketStats(perctile_bucket_t *b) {
	DEFiRet;

	CHKiRet(statsobj.Construct(&b->statsobj));
	CHKiRet(statsobj.SetOrigin(b->statsobj, UCHAR_CONSTANT("percstats.bucket")));
	CHKiRet(statsobj.SetName(b->statsobj, b->name));
	CHKiRet(statsobj.SetReportingNamespace(b->statsobj, UCHAR_CONSTANT("values")));
	statsobj.SetReadNotifier(b->statsobj, perctile_readCallback, b);
	CHKiRet(statsobj.ConstructFinalize(b->statsobj));

finalize_it:
	RETiRet;
}

static rsRetVal
perctileAddBucketMetrics(perctile_buckets_t *bkts, perctile_bucket_t *b, const uchar* name) {
	uchar *metric_name_buff, *metric_suffix;
	const uchar *suffix_litteral;
	int name_len;
	DEFiRet;

	name_len = ustrlen(name);
	CHKmalloc(metric_name_buff = malloc((name_len + PERCTILE_MAX_BUCKET_NS_METRIC_LENGTH + 1) * sizeof(uchar)));

	strcpy((char*)metric_name_buff, (char*)name);
	metric_suffix = metric_name_buff + name_len;
	*metric_suffix = PERCTILE_METRIC_NAME_SEPARATOR;
	metric_suffix++;

	suffix_litteral = UCHAR_CONSTANT("new_metric_add");
	ustrncpy(metric_suffix, suffix_litteral, PERCTILE_MAX_BUCKET_NS_METRIC_LENGTH);
	STATSCOUNTER_INIT(b->ctrNewKeyAdd, b->mutCtrNewKeyAdd);
	CHKiRet(statsobj.AddManagedCounter(bkts->global_stats, metric_name_buff, ctrType_IntCtr,
				CTR_FLAG_RESETTABLE,
				&(b->ctrNewKeyAdd),
				&b->pNewKeyAddCtr, 1));

	suffix_litteral = UCHAR_CONSTANT("ops_overflow");
	ustrncpy(metric_suffix, suffix_litteral, PERCTILE_MAX_BUCKET_NS_METRIC_LENGTH);
	STATSCOUNTER_INIT(b->ctrOpsOverflow, b->mutCtrOpsOverflow);
	CHKiRet(statsobj.AddManagedCounter(bkts->global_stats, metric_name_buff, ctrType_IntCtr,
				CTR_FLAG_RESETTABLE,
				&(b->ctrOpsOverflow),
				&b->pOpsOverflowCtr, 1));

finalize_it:
	free(metric_name_buff);
	if (iRet != RS_RET_OK) {
		if (b->pOpsOverflowCtr != NULL) {
			statsobj.DestructCounter(bkts->global_stats, b->pOpsOverflowCtr);
		}
		if (b->pNewKeyAddCtr != NULL) {
			statsobj.DestructCounter(bkts->global_stats, b->pNewKeyAddCtr);
		}
	}
	RETiRet;
}

/* Create new perctile bucket, and add it to our list of perctile buckets.
*/
static rsRetVal
perctile_newBucket(const uchar *name, uint8_t *perctiles, uint32_t perctilesCount, uint32_t windowSize) {
	perctile_buckets_t *bkts;
	perctile_bucket_t* b = NULL;
	pthread_rwlockattr_t bucket_lock_attr;
	DEFiRet;

	bkts = &loadConf->perctile_buckets;

	if (bkts->initialized)
	{
		CHKmalloc(b = calloc(1, sizeof(perctile_bucket_t)));

		// initialize
		pthread_rwlock_init(&b->lock, &bucket_lock_attr);
		CHKmalloc(b->htable = create_hashtable(7, hash_from_string, key_equals_string, perctileStatDestruct));
		CHKmalloc(b->name = ustrdup(name));
		b->perctile_values = perctiles;
		CHKmalloc(b->perctile_values = calloc(perctilesCount, sizeof(u_int8_t)));
		b->perctile_values_count = perctilesCount;
		memcpy(b->perctile_values, perctiles, perctilesCount * sizeof(uint8_t));
		b->window_size = windowSize;
		b->next = NULL;
		PERCTILE_STATS_LOG("perctile_newBucket: create new bucket for %s, with windowsize: %d,  values_count: %zu\n",
				b->name, b->window_size, b->perctile_values_count);

		// add bucket to list of buckets
		if (!bkts->listBuckets)
		{
			// none yet
			bkts->listBuckets = b;
			PERCTILE_STATS_LOG("perctile_newBucket: Adding new bucket to empty list \n");
		}
		else
		{
			b->next = bkts->listBuckets;
			bkts->listBuckets = b;
			PERCTILE_STATS_LOG("perctile_newBucket: prepended new bucket list \n");
		}

		// assert we can find the newly added bucket
		{
			perctile_bucket_t *pb = findBucket(bkts->listBuckets, name);
			assert(pb);
		}

		// create the statsobj for this bucket
		CHKiRet(perctileInitNewBucketStats(b));
		CHKiRet(perctileAddBucketMetrics(bkts, b, name));
	}
	else
	{
		LogError(0, RS_RET_INTERNAL_ERROR, "perctile: bucket creation failed, as "
				"global-initialization of buckets was unsuccessful");
		ABORT_FINALIZE(RS_RET_INTERNAL_ERROR);
	}
finalize_it:
	if (iRet != RS_RET_OK)
	{
		if (b != NULL) {
			perctileBucketDestruct(b);
		}
	}
	RETiRet;
}

// Public functions
rsRetVal
perctile_processCnf(struct cnfobj *o) {
	struct cnfparamvals *pvals;
	uchar *name = NULL;
	uint8_t *perctiles = NULL;
	uint32_t perctilesCount = 0;
	uint64_t windowSize = 0;
	DEFiRet;

	pvals = nvlstGetParams(o->nvlst, &modpblk, NULL);
	if(pvals == NULL) {
		ABORT_FINALIZE(RS_RET_MISSING_CNFPARAMS);
	}

	for(short i = 0 ; i < modpblk.nParams ; ++i) {
		if(!pvals[i].bUsed)
			continue;
		if(!strcmp(modpblk.descr[i].name, PERCTILE_PARAM_NAME)) {
			CHKmalloc(name = (uchar*)es_str2cstr(pvals[i].val.d.estr, NULL));
		} else if (!strcmp(modpblk.descr[i].name, PERCTILE_PARAM_PERCENTILES)) {
			perctilesCount = pvals[i].val.d.ar->nmemb;
			if (perctilesCount) {
				CHKmalloc(perctiles = calloc(perctilesCount, sizeof(uint32_t)));
				for (int j = 0; j < pvals[i].val.d.ar->nmemb; ++j) {
					char *cstr = es_str2cstr(pvals[i].val.d.ar->arr[j], NULL);
					perctiles[j] = atoi(cstr);
					free(cstr);
				}
			}
		} else if (!strcmp(modpblk.descr[i].name, PERCTILE_PARAM_WINDOW_SIZE)) {
			windowSize = pvals[i].val.d.n;
		} else {
			dbgprintf("perctile: program error, non-handled "
					"param '%s'\n", modpblk.descr[i].name);
		}
	}

	if (name != NULL) {
		CHKiRet(perctile_newBucket(name, perctiles, perctilesCount, windowSize));
	}

finalize_it:
	free(name);
	free(perctiles);
	cnfparamvalsDestruct(pvals, &modpblk);
	RETiRet;
}

rsRetVal
perctile_initCnf(perctile_buckets_t *bkts) {
	DEFiRet;

	bkts->initialized = 0;
	bkts->listBuckets = NULL;
	CHKiRet(statsobj.Construct(&bkts->global_stats));
	CHKiRet(statsobj.SetOrigin(bkts->global_stats, UCHAR_CONSTANT("perctile")));
	CHKiRet(statsobj.SetName(bkts->global_stats, UCHAR_CONSTANT("global")));
	CHKiRet(statsobj.SetReportingNamespace(bkts->global_stats, UCHAR_CONSTANT("values")));
	CHKiRet(statsobj.ConstructFinalize(bkts->global_stats));
	pthread_rwlock_init(&bkts->lock, NULL);
	bkts->initialized = 1;

finalize_it:
	if (iRet != RS_RET_OK) {
		statsobj.Destruct(&bkts->global_stats);
	}
	RETiRet;
}

perctile_bucket_t*
perctile_findBucket(const uchar* name) {
	perctile_bucket_t *b = NULL;

	perctile_buckets_t *bkts = &loadConf->perctile_buckets;
	if (bkts->initialized) {
		pthread_rwlock_rdlock(&bkts->lock);
		b = findBucket(bkts->listBuckets, name);
		assert(b);
		pthread_rwlock_unlock(&bkts->lock);
	} else {
		LogError(0, RS_RET_INTERNAL_ERROR, "perctile: bucket lookup failed, as global-initialization "
				"of buckets was unsuccessful");
	}
	return b;
}

rsRetVal
perctile_obs(perctile_bucket_t *perctile_bkt, uchar* key, int64_t value) {
	DEFiRet;
	if (!perctile_bkt) {
		LogError(0, RS_RET_INTERNAL_ERROR, "perctile() - perctile bkt not available");
		FINALIZE;
	}
	PERCTILE_STATS_LOG("perctile_obs() - bucket name: %s, key: %s, val: %lld\n", perctile_bkt->name, key, value);

	CHKiRet(perctile_observe(perctile_bkt, key, value));

finalize_it:
	if (iRet != RS_RET_OK) {
		// free pstat
		assert(0);
	}
	RETiRet;
}

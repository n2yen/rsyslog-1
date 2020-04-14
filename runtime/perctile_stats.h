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

#ifndef INCLUDED_PERCTILE_STATS_H
#define INCLUDED_PERCTILE_STATS_H

#include "hashtable.h"
#include "statsobj.h"

// TODO: revisit if move to our own module to remove this dependency
//#include "dynstats.h"

#define PERCTILE_PARAM_TYPE 			 "type"
#define PERCTILE_PARAM_PERCENTILES "percentiles"
#define PERCTILE_PARAM_WINDOW_SIZE "windowsize"

typedef int64_t ITEM;

struct circ_buf {
  ITEM *buf;
  int head;
  int tail;
};

typedef struct ringbuf_s {
  struct circ_buf cb;
  size_t size;
} ringbuf_t;

struct perctile_ctr_s {
	uchar* name;
	// converted from percentile
	uint8_t percentile;
	size_t index;  // todo: use this to cache the percentile offset.
	//STATSCOUNTER_DEF(ctrPerctileVal, mutCtrPerctileVal);
	// this technically isn't a counter, but a value that should be set

	// this is the actual calculated percentile value
	intctr_t perctile_stat;
};

struct perctile_stat_s {
	uchar *name;

	// TODO: create a lock for ringbuffer.
	pthread_rwlock_t rb_lock;
	ringbuf_t *rb_observed_stats;

	// TODO: create a lock for my counters
	pthread_rwlock_t stats_lock;
	// contains an array of requested perctile to track
	struct perctile_ctr_s *ctrs;
	size_t perctile_ctrs_count;

	intctr_t ctrWindowCount;
	intctr_t ctrWindowMin;
	intctr_t ctrWindowMax;
	intctr_t ctrWindowSum;
	intctr_t ctrHistoricalWindowCount;
	intctr_t ctrHistoricalWindowMin; 
	intctr_t ctrHistoricalWindowMax; 
	intctr_t ctrHistoricalWindowSum; 
};

struct perctile_bucket_s {
	uchar *name;

	// lock for entire bucket
	pthread_rwlock_t lock;
	struct hashtable *htable;
	struct perctile_bucket_s *next;
	statsobj_t *statsobj;

	u_int32_t window_size;
	// maybe some global stats
	// some historical values may be needed, but they are bucket

	// These percentile values apply to all perctile stats in this bucket.
	uint8_t *perctile_values;
	size_t perctile_values_count;
};

// Not needed for now, since
// we are just leveraging the dynstats_buckets_s
// as the container of everything.
// We can enable this if we create a separate 
// module

struct perctile_buckets_s {
	u_int8_t initialized;
	statsobj_t *global_stats;
	pthread_rwlock_t lock;
	struct perctile_bucket_s *listBuckets;
};

// TODO: dynstats, has these typedefs in typedefs.h
// keep these local for now.
typedef struct perctile_bucket_s perctile_bucket_t;
typedef struct perctile_stat_s perctile_stat_t;
typedef struct perctile_ctr_s perctile_ctr_t;
//typedef struct perctile_buckets_s perctile_buckets_t;

rsRetVal perctile_processCnf(struct cnfobj *cnf);
rsRetVal perctile_observe(perctile_bucket_t *bkt, uchar* stat_name, int64_t value);
rsRetVal perctile_publish(void);

// to be determined if needed.
//perctile_bucket_t* perctile_findBucket(perctile_bucket_t *head, const uchar *name);
void perctile_readCallback(statsobj_t __attribute__((unused)) *ignore, void *allbuckets);

// These would only be needed for own module
rsRetVal perctileClassInit(void);

/* new perctile related functions */
rsRetVal perctile_initCnf(perctile_buckets_t *b);
perctile_bucket_t* perctile_findBucket(const uchar* name);
rsRetVal perctile_obs(perctile_bucket_t *perctile_bkt, uchar* key, int64_t value); 
rsRetVal perctileInitNewBucketStats(perctile_bucket_t *b);
rsRetVal initAndAddPerctileMetrics(perctile_bucket_t *bkt, perctile_stat_t *pstat);

#endif /* #ifndef INCLUDED_PERCTILE_STATS_H */
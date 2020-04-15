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

struct perctile_ctr_s {
	uchar* name;
	// percentile [0,100]
	uint8_t percentile;
	size_t index;
	intctr_t perctile_stat;
};

struct perctile_stat_s {
	uchar *name;

	// not needed for now.
	//pthread_rwlock_t rb_lock;
	struct ringbuf_s *rb_observed_stats;

	// array of requested perctile to track
	struct perctile_ctr_s *ctrs;
	size_t perctile_ctrs_count;

	pthread_rwlock_t stats_lock;
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
	STATSCOUNTER_DEF(ctrNewKeyAdd, mutCtrNewKeyAdd);
	ctr_t *pNewKeyAddCtr;
	STATSCOUNTER_DEF(ctrOpsOverflow, mutCtrOpsOverflow);
	ctr_t *pOpsOverflowCtr;

	u_int32_t window_size;

	// These percentile values apply to all perctile stats in this bucket.
	uint8_t *perctile_values;
	size_t perctile_values_count;
};

struct perctile_buckets_s {
	u_int8_t initialized;
	statsobj_t *global_stats;
	pthread_rwlock_t lock;
	struct perctile_bucket_s *listBuckets;
};

// keep these local for now.
typedef struct perctile_ctr_s perctile_ctr_t;
typedef struct perctile_stat_s perctile_stat_t;
typedef struct perctile_bucket_s perctile_bucket_t;

rsRetVal perctileClassInit(void);
void perctileBucketsDestruct();
rsRetVal perctile_initCnf(perctile_buckets_t *b);
perctile_bucket_t* perctile_findBucket(const uchar* name);
rsRetVal perctile_processCnf(struct cnfobj *cnf);
rsRetVal perctile_obs(perctile_bucket_t *perctile_bkt, uchar* key, int64_t value);

#endif /* #ifndef INCLUDED_PERCTILE_STATS_H */

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

// support stats gathering
//DEFobjCurrIf(statsobj)

static uint64_t min(uint64_t a, uint64_t b) {
  return a < b ? a : b;
}

static uint64_t max(uint64_t a, uint64_t b) {
  return a > b ? a : b;
}

//#define PERCTILE_STATS_DEBUG
#ifdef PERCTILE_STATS_DEBUG
#define DEBUG 1
#else
#define DEBUG 0
#endif
#define PERCTILE_STATS_LOG(...) do { if(DEBUG) fprintf(stderr, __VA_ARGS__); } while(0)

// simple use of the linux defined circular buffer.
// TODO: Create a module specific struct outside of this
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
  size_t bytes = 0;
  bytes += ringbuf_read(rb, buf, count);
  if (bytes == 0) {
    return bytes;
  }
  // read the rest if buf circled around
  bytes += ringbuf_read(rb, buf+bytes, count);
  return bytes;
}

bool ringbuf_peek(ringbuf_t *rb, ITEM *item) {
  if (CIRC_CNT(rb->cb.head, rb->cb.tail, rb->size) == 0) {
    return false;
  }

  *item = rb->cb.buf[rb->cb.head];
  return true;
}


// TODO: this is fully implemented yet.
static void perctile_DestroyPerctileStat(void* p) {
  if (p) {
    perctile_stat_t *perc_stat = (perctile_stat_t *)p;
    if (perc_stat->name) {
      free(perc_stat->name);
    }
    if (perc_stat->rb_observed_stats) {
			ringbuf_del(perc_stat->rb_observed_stats);
    }
    if (perc_stat->ctrs) {
			free(perc_stat->ctrs);
    }
		pthread_rwlock_destroy(&perc_stat->stats_lock);
		pthread_rwlock_destroy(&perc_stat->rb_lock);
		free(perc_stat);
  }
}

static void perctile_DestroyBucket(perctile_bucket_t *bkt) {
  PERCTILE_STATS_LOG("destructing perctile bucket\n");
  if (bkt) {
    free(bkt->name);
    free(bkt->perctile_values);
    // lock
    pthread_rwlock_wrlock(&bkt->lock);
    // destrcut statsobj
    dynstats_perctileDestroyPerctileStats(bkt->statsobj);

    hashtable_destroy(bkt->htable, 1); // destroy all perctile stats
    pthread_rwlock_unlock(&bkt->lock);
    free(bkt);
  }
}

// This needs to actually to set the pointer somewhere. should it return it?
/*
static rsRetVal perctile_newPerctileStat(uchar* name, uint32_t *percentiles, uint32_t percentileCount) {
  pthread_rwlockattr_t attr;
  DEFiRet;
  // fill percentile 
  perctile_stat_t *perc_stat = calloc(1, sizeof(perctile_stat_t));

  perc_stat->name = name;
  pthread_rwlock_init(&perc_stat->rwlock, &attr);

  // init ringuffer

  RETiRet;
}
*/

void pertile_destroyAllBuckets(dynstats_buckets_t *bkts) {
  perctile_bucket_t *head = bkts->listPerctileBuckets;

	if (bkts->initialized) {
		if (head) {
			perctile_bucket_t *pnode = NULL;
      pthread_rwlock_trywrlock(&bkts->perctile_lock);
			for (perctile_bucket_t *phead = head; phead != NULL; phead = phead->next) {
				pnode = phead;
				perctile_DestroyBucket(pnode);
			}
		}
    // destroy any global stats we keep specifically for this.
    pthread_rwlock_unlock(&bkts->perctile_lock);
    pthread_rwlock_destroy(&bkts->perctile_lock);
	}
}

perctile_bucket_t* perctile_findBucket(perctile_bucket_t *head, const uchar *name) {
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

static void print_perctiles(perctile_bucket_t *bkt) {
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

rsRetVal perctile_observe(perctile_bucket_t *bkt, uchar* key, int64_t value) {
  DEFiRet;

  pthread_rwlock_wrlock(&bkt->lock);
	perctile_stat_t *pstat = (perctile_stat_t*) hashtable_search(bkt->htable, key);
	uchar* hash_key = NULL;
	if (!pstat) {
		PERCTILE_STATS_LOG("perctile_observe(): key '%s' not found - creating new pstat", key);
		// create the pstat if not found
		CHKmalloc(pstat = calloc(1, sizeof(perctile_stat_t)));
		CHKmalloc(pstat->name = ustrdup(key));
		CHKmalloc(pstat->ctrs = (perctile_ctr_t*)calloc(bkt->perctile_values_count, sizeof(perctile_stat_t)));
    pstat->perctile_ctrs_count = bkt->perctile_values_count;
	  CHKmalloc(pstat->rb_observed_stats = ringbuf_new(bkt->window_size));
		pthread_rwlock_init(&pstat->rb_lock, NULL);
		pthread_rwlock_init(&pstat->stats_lock, NULL);

		// init all stat counters here - TODO: do we need to use this macro since we are essentially using
    // this as a gauge.
    // These need to be added at construction time.
		pstat->ctrWindowCount = pstat->ctrWindowMax = pstat->ctrWindowSum = 0;
		pstat->ctrWindowMin = sizeof(pstat->ctrWindowMin);
    assert(pstat->ctrWindowMin != 0);

		pstat->ctrHistoricalWindowCount = pstat->ctrHistoricalWindowMax =  pstat->ctrHistoricalWindowSum = 0;
		pstat->ctrHistoricalWindowMin = sizeof(pstat->ctrHistoricalWindowMin);

		CHKiRet(dynstats_initAndAddPerctileMetrics(bkt, pstat));
		CHKmalloc(hash_key = ustrdup(key));
		if (!hashtable_insert(bkt->htable, ustrdup(key), pstat)) {
			free(hash_key);
			assert(0);
      FINALIZE;
		}
  	PERCTILE_STATS_LOG("perctile_observe - new pstat created - name: %s\n", pstat->name);
  }

	perctile_stat_t *find = (perctile_stat_t*) hashtable_search(bkt->htable, key);
	assert(find);
	assert(ustrcmp(find->name, key) == 0);

	// add this value into the ringbuffer
  assert(pstat->rb_observed_stats);
  if (ringbuf_append_with_overwrite(pstat->rb_observed_stats, value) != 0) {
    // ringbuffer is operating in overwrite mode, so should never see this.
    iRet = -1;
    assert(0);
    FINALIZE;
  }
  // update perctile specific stats
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
  /*
  PERCTILE_STATS_LOG("perctile_observe - appended value: %lld to ringbuffer\n", value);
	PERCTILE_STATS_LOG("ringbuffer contents... \n");
	for (size_t i = 0; i < pstat->rb_observed_stats->size; ++i) {
		PERCTILE_STATS_LOG("%lld ", pstat->rb_observed_stats->cb.buf[i]);
	}
	PERCTILE_STATS_LOG("\n");
  */
	//print_perctiles(bkt);
finalize_it:
  pthread_rwlock_unlock(&bkt->lock);
  if (iRet != RS_RET_OK) {
    // clean up if there was an error
  }
  RETiRet;
}

int cmp(const void* p1, const void* p2) {
  return (*(ITEM*)p1) - (*(ITEM*)p2);
}

static void report_perctile_stats(perctile_bucket_t* pbkt) {
  ITEM *buf = NULL;
  uint8_t lock_initialized = 0;
  DEFiRet;

  pthread_rwlock_rdlock(&pbkt->lock);
	if (hashtable_count(pbkt->htable)) {
		struct hashtable_itr *itr = hashtable_iterator(pbkt->htable);
		CHKmalloc(buf = malloc(pbkt->window_size*sizeof(ITEM)));
		do {
			memset(buf, 0, pbkt->window_size*sizeof(ITEM));
			perctile_stat_t *perc_stat = hashtable_iterator_value(itr);
			// ringbuffer read
			size_t bytes = ringbuf_read_to_end(perc_stat->rb_observed_stats, buf, pbkt->window_size);
      perc_stat->ctrWindowCount = 0;
			if (!bytes) {
				FINALIZE;
				assert(0);
			}
			PERCTILE_STATS_LOG("read %zu bytes\n", bytes);
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
			qsort(buf, bytes, sizeof(ITEM), cmp);

			PERCTILE_STATS_LOG("buffer contents after sort... \n");
			for (size_t i = 0; i < perc_stat->rb_observed_stats->size; ++i) {
				PERCTILE_STATS_LOG("%lld ", buf[i]);
			}
			PERCTILE_STATS_LOG("\n");

			PERCTILE_STATS_LOG("report_perctile_stats() - perctile stat has %zu counters.", perc_stat->perctile_ctrs_count);
			for (size_t i = 0; i < perc_stat->perctile_ctrs_count; ++i) {
				perctile_ctr_t *pctr = &perc_stat->ctrs[i];
				// get percentile - this can be cached.
				int index = ((pctr->percentile/100.0) * bytes)-1;
				// look into if we need to lock this.
				pctr->perctile_stat = buf[index];
				PERCTILE_STATS_LOG("report_perctile_stats() - index: %d, perctile stat [%s, %d, %llu]", index, pctr->name, pctr->percentile, pctr->perctile_stat);
			}
		} while (hashtable_iterator_advance(itr));
	}

finalize_it:
  pthread_rwlock_unlock(&pbkt->lock);
  free(buf);
}

void perctile_readCallback(statsobj_t __attribute__((unused)) *ignore, void *allbuckets) {
  // induce the lock here.
  dynstats_buckets_t *bkts = (dynstats_buckets_t *)allbuckets;

  // walk through all buckets and report on the current p95
  pthread_rwlock_rdlock(&bkts->lock);
  for (perctile_bucket_t *pbkt = bkts->listPerctileBuckets; pbkt != NULL; pbkt = pbkt->next) {
    report_perctile_stats(pbkt);
  }
  pthread_rwlock_unlock(&bkts->lock);

}

/*
rsRetVal perctile_initNewBucketStats(dynstats_buckets_t *bkts, perctile_bucket_t *b) {
	DEFiRet;
	
	CHKiRet(statsobj.Construct(&b->statsobj));
  // TODO: determine if this should be renamed.
	CHKiRet(statsobj.SetOrigin(b->statsobj, UCHAR_CONSTANT("dynstats.bucket")));
	CHKiRet(statsobj.SetName(b->statsobj, b->name));
	CHKiRet(statsobj.SetReportingNamespace(b->statsobj, UCHAR_CONSTANT("values")));
	statsobj.SetReadNotifier(b->statsobj, perctile_readCallback, bkts);
	CHKiRet(statsobj.ConstructFinalize(b->stats));
	
finalize_it:
	RETiRet;

}
*/

/* Create new perctile bucket, and add it to our list of perctile buckets.
*/
rsRetVal perctile_newBucket(dynstats_buckets_t *bkts, const uchar *name, uint8_t *perctiles,
                            uint32_t perctilesCount, uint64_t windowSize)
{
  uint8_t lock_initialized = 0,
					metric_count_mutex_initialized = 0;
  perctile_bucket_t* b = NULL;
	pthread_rwlockattr_t bucket_lock_attr;
  DEFiRet;

  CHKmalloc(b = calloc(1, sizeof(perctile_bucket_t)));

  // initialize
  // for each percentile, we need to create a
  pthread_rwlockattr_init(&bucket_lock_attr);
	lock_initialized = 1;
#ifdef HAVE_PTHREAD_RWLOCKATTR_SETKIND_NP
  pthread_rwlockattr_setkind_np(&bucket_lock_attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
#endif  
  CHKmalloc(b->htable = create_hashtable(7, hash_from_string, key_equals_string, perctile_DestroyPerctileStat));
	CHKmalloc(b->name = ustrdup(name));
  b->perctile_values = perctiles;
	CHKmalloc(b->perctile_values = calloc(perctilesCount, sizeof(u_int8_t)));
  b->perctile_values_count = perctilesCount;
	memcpy(b->perctile_values, perctiles, perctilesCount*sizeof(uint8_t));
  b->window_size = windowSize;
  b->next = NULL;
  PERCTILE_STATS_LOG("perctile_newBucket: create new bucket for %s, with windowsize: %d,  values_count: %zu",
    b->name, b->window_size, b->perctile_values_count);

  // add bucket to list of buckets
  if (!bkts->listPerctileBuckets) {
    // none yet
    bkts->listPerctileBuckets = b;
    PERCTILE_STATS_LOG("perctile_newBucket: Adding new bucket to empty list ");
  } else {
    b->next = bkts->listPerctileBuckets;
    bkts->listPerctileBuckets = b;
    PERCTILE_STATS_LOG("perctile_newBucket: prepended new bucket list ");
  }

  // assert we can find the newly added bucket
  {
	  perctile_bucket_t* pb = perctile_findBucket(bkts->listPerctileBuckets, name);
    assert(pb);
  }

  // create the statsobj for this bucket
	CHKiRet(dynstats_perctileInitNewBucketStats(bkts, b));
  pthread_rwlock_unlock(&b->lock);

finalize_it:
	if (iRet != RS_RET_OK)
	{
		//if (metric_count_mutex_initialized)
		//{
		//	pthread_mutex_destroy(&b->mutMetricCount);
		//}
		if (lock_initialized)
		{
			pthread_rwlock_destroy(&b->lock);
		}
		if (b != NULL)
		{
			perctile_DestroyBucket(b);
		}
	}
  RETiRet;
}
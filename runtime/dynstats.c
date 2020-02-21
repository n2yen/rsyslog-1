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
#include <unistd.h>
#include <fcntl.h>
#include <json.h>
#include <sys/stat.h>

#include "rsyslog.h"
#include "srUtils.h"
#include "errmsg.h"
#include "rsconf.h"
#include "unicode-helper.h"
#include "hashtable_itr.h"

/* definitions for objects we access */
DEFobjStaticHelpers
DEFobjCurrIf(statsobj)

#define DYNSTATS_PARAM_NAME "name"
#define DYNSTATS_PARAM_RESETTABLE "resettable"
#define DYNSTATS_PARAM_MAX_CARDINALITY "maxCardinality"
#define DYNSTATS_PARAM_UNUSED_METRIC_LIFE "unusedMetricLife" /* in seconds */
#define DYNSTATS_PARAM_PERSISTSTATEINTERVAL "persistStateInterval"
#define DYNSTATS_PARAM_STATEFILE_DIRECTORY "statefile.directory"

#define DYNSTATS_DEFAULT_RESETTABILITY 1
#define DYNSTATS_DEFAULT_MAX_CARDINALITY 2000
#define DYNSTATS_DEFAULT_UNUSED_METRIC_LIFE 3600 /* seconds */
#define DYNSTATS_DEFAULT_PERSISTSTATEINTERVAL 0  /* seconds, default is 0, or never */

#define DYNSTATS_MAX_BUCKET_NS_METRIC_LENGTH 100
#define DYNSTATS_METRIC_NAME_SEPARATOR '.'
#define DYNSTATS_HASHTABLE_SIZE_OVERPROVISIONING 1.25
#define DYNSTATS_BUCKET_PERSIST_NAME "name"
#define DYNSTATS_BUCKET_PERSIST_VALUES_NAME "values"

static struct cnfparamdescr modpdescr[] = {
	{ DYNSTATS_PARAM_NAME, eCmdHdlrString, CNFPARAM_REQUIRED },
	{ DYNSTATS_PARAM_RESETTABLE, eCmdHdlrBinary, 0 },
	{ DYNSTATS_PARAM_MAX_CARDINALITY, eCmdHdlrPositiveInt, 0},
	{ DYNSTATS_PARAM_UNUSED_METRIC_LIFE, eCmdHdlrPositiveInt, 0}, /* in minutes */
	{ DYNSTATS_PARAM_PERSISTSTATEINTERVAL, eCmdHdlrPositiveInt, 0 },
	{ DYNSTATS_PARAM_STATEFILE_DIRECTORY, eCmdHdlrString, 0 }
};

static struct cnfparamblk modpblk =
{
	CNFPARAMBLK_VERSION,
	sizeof(modpdescr)/sizeof(struct cnfparamdescr),
	modpdescr
};

static rsRetVal dynstats_addNewCtr(dynstats_bucket_t *b, const uchar* metric, uint8_t doInitialIncrement,
		uint64_t doInitialOffset);
static int getFullStateFileName(dynstats_bucket_t *b, uchar* pszstatefile, uchar* pszout, int buflen);
static uchar* getStateFileName(const dynstats_bucket_t *const pbucket,
		uchar *const __restrict__ buf, const size_t lenbuf) ATTR_NONNULL(1, 2);
static rsRetVal persistBucketState(dynstats_bucket_t *b) ATTR_NONNULL(1);

rsRetVal
dynstatsClassInit(void) {
	DEFiRet;
	CHKiRet(objGetObjInterface(&obj));
	CHKiRet(objUse(statsobj, CORE_COMPONENT));
finalize_it:
	RETiRet;
}

static void
dynstats_destroyCtr(dynstats_ctr_t *ctr) {
	statsobj.DestructUnlinkedCounter(ctr->pCtr);
	free(ctr->metric);
	free(ctr);
}

static void /* assumes exclusive access to bucket */
dynstats_destroyCountersIn(dynstats_bucket_t *b, htable *table, dynstats_ctr_t *ctrs) {
	dynstats_ctr_t *ctr;
	int ctrs_purged = 0;
	hashtable_destroy(table, 0);
	while (ctrs != NULL) {
		ctr = ctrs;
		ctrs = ctrs->next;
		dynstats_destroyCtr(ctr);
		ctrs_purged++;
	}
	STATSCOUNTER_ADD(b->ctrMetricsPurged, b->mutCtrMetricsPurged, ctrs_purged);
	ATOMIC_SUB_unsigned(&b->metricCount, ctrs_purged, &b->mutMetricCount);
}

static void /* assumes exclusive access to bucket */
dynstats_destroyCounters(dynstats_bucket_t *b) {
	statsobj.UnlinkAllCounters(b->stats);
	dynstats_destroyCountersIn(b, b->table, b->ctrs);
}

static void
dynstats_destroyBucket(dynstats_bucket_t* b) {
	dynstats_buckets_t *bkts;

	bkts = &loadConf->dynstats_buckets;

	if(b->persistStateInterval) {
		persistBucketState(b);
	}
	pthread_rwlock_wrlock(&b->lock);
	dynstats_destroyCounters(b);
	dynstats_destroyCountersIn(b, b->survivor_table, b->survivor_ctrs);
	statsobj.Destruct(&b->stats);
	free(b->name);
	free(b->stateFileDirectory);
	pthread_rwlock_unlock(&b->lock);
	pthread_rwlock_destroy(&b->lock);
	pthread_mutex_destroy(&b->mutMetricCount);
	statsobj.DestructCounter(bkts->global_stats, b->pOpsOverflowCtr);
	statsobj.DestructCounter(bkts->global_stats, b->pNewMetricAddCtr);
	statsobj.DestructCounter(bkts->global_stats, b->pNoMetricCtr);
	statsobj.DestructCounter(bkts->global_stats, b->pMetricsPurgedCtr);
	statsobj.DestructCounter(bkts->global_stats, b->pOpsIgnoredCtr);
	statsobj.DestructCounter(bkts->global_stats, b->pPurgeTriggeredCtr);
	free(b);
}

static rsRetVal
dynstats_addBucketMetrics(dynstats_buckets_t *bkts, dynstats_bucket_t *b, const uchar* name) {
	uchar *metric_name_buff, *metric_suffix;
	const uchar *suffix_litteral;
	int name_len;
	DEFiRet;

	name_len = ustrlen(name);
	CHKmalloc(metric_name_buff = malloc((name_len + DYNSTATS_MAX_BUCKET_NS_METRIC_LENGTH + 1) * sizeof(uchar)));

	strcpy((char*)metric_name_buff, (char*)name);
	metric_suffix = metric_name_buff + name_len;
	*metric_suffix = DYNSTATS_METRIC_NAME_SEPARATOR;
	metric_suffix++;

	suffix_litteral = UCHAR_CONSTANT("ops_overflow");
	ustrncpy(metric_suffix, suffix_litteral, DYNSTATS_MAX_BUCKET_NS_METRIC_LENGTH);
	STATSCOUNTER_INIT(b->ctrOpsOverflow, b->mutCtrOpsOverflow);
	CHKiRet(statsobj.AddManagedCounter(bkts->global_stats, metric_name_buff, ctrType_IntCtr,
									   CTR_FLAG_RESETTABLE,
										&(b->ctrOpsOverflow),
										&b->pOpsOverflowCtr, 1));

	suffix_litteral = UCHAR_CONSTANT("new_metric_add");
	ustrncpy(metric_suffix, suffix_litteral, DYNSTATS_MAX_BUCKET_NS_METRIC_LENGTH);
	STATSCOUNTER_INIT(b->ctrNewMetricAdd, b->mutCtrNewMetricAdd);
	CHKiRet(statsobj.AddManagedCounter(bkts->global_stats, metric_name_buff, ctrType_IntCtr,
									   CTR_FLAG_RESETTABLE,
										&(b->ctrNewMetricAdd),
										&b->pNewMetricAddCtr, 1));

	suffix_litteral = UCHAR_CONSTANT("no_metric");
	ustrncpy(metric_suffix, suffix_litteral, DYNSTATS_MAX_BUCKET_NS_METRIC_LENGTH);
	STATSCOUNTER_INIT(b->ctrNoMetric, b->mutCtrNoMetric);
	CHKiRet(statsobj.AddManagedCounter(bkts->global_stats, metric_name_buff, ctrType_IntCtr,
									   CTR_FLAG_RESETTABLE,
										&(b->ctrNoMetric),
										&b->pNoMetricCtr, 1));

	suffix_litteral = UCHAR_CONSTANT("metrics_purged");
	ustrncpy(metric_suffix, suffix_litteral, DYNSTATS_MAX_BUCKET_NS_METRIC_LENGTH);
	STATSCOUNTER_INIT(b->ctrMetricsPurged, b->mutCtrMetricsPurged);
	CHKiRet(statsobj.AddManagedCounter(bkts->global_stats, metric_name_buff, ctrType_IntCtr,
									   CTR_FLAG_RESETTABLE,
										&(b->ctrMetricsPurged),
										&b->pMetricsPurgedCtr, 1));

	suffix_litteral = UCHAR_CONSTANT("ops_ignored");
	ustrncpy(metric_suffix, suffix_litteral, DYNSTATS_MAX_BUCKET_NS_METRIC_LENGTH);
	STATSCOUNTER_INIT(b->ctrOpsIgnored, b->mutCtrOpsIgnored);
	CHKiRet(statsobj.AddManagedCounter(bkts->global_stats, metric_name_buff, ctrType_IntCtr,
									   CTR_FLAG_RESETTABLE,
										&(b->ctrOpsIgnored),
										&b->pOpsIgnoredCtr, 1));

	suffix_litteral = UCHAR_CONSTANT("purge_triggered");
	ustrncpy(metric_suffix, suffix_litteral, DYNSTATS_MAX_BUCKET_NS_METRIC_LENGTH);
	STATSCOUNTER_INIT(b->ctrPurgeTriggered, b->mutCtrPurgeTriggered);
	CHKiRet(statsobj.AddManagedCounter(bkts->global_stats, metric_name_buff, ctrType_IntCtr,
									   CTR_FLAG_RESETTABLE,
										&(b->ctrPurgeTriggered),
										&b->pPurgeTriggeredCtr, 1));

finalize_it:
	free(metric_name_buff);
	if (iRet != RS_RET_OK) {
		if (b->pOpsOverflowCtr != NULL) {
			statsobj.DestructCounter(bkts->global_stats, b->pOpsOverflowCtr);
		}
		if (b->pNewMetricAddCtr != NULL) {
			statsobj.DestructCounter(bkts->global_stats, b->pNewMetricAddCtr);
		}
		if (b->pNoMetricCtr != NULL) {
			statsobj.DestructCounter(bkts->global_stats, b->pNoMetricCtr);
		}
		if (b->pMetricsPurgedCtr != NULL) {
			statsobj.DestructCounter(bkts->global_stats, b->pMetricsPurgedCtr);
		}
		if (b->pOpsIgnoredCtr != NULL) {
			statsobj.DestructCounter(bkts->global_stats, b->pOpsIgnoredCtr);
		}
		if (b->pPurgeTriggeredCtr != NULL) {
			statsobj.DestructCounter(bkts->global_stats, b->pPurgeTriggeredCtr);
		}
	}
	RETiRet;
}

static void
no_op_free(void __attribute__((unused)) *ignore)  {}

static rsRetVal  /* assumes exclusive access to bucket */
dynstats_rebuildSurvivorTable(dynstats_bucket_t *b) {
	htable *survivor_table = NULL;
	htable *new_table = NULL;
	size_t htab_sz;
	DEFiRet;
	
	htab_sz = (size_t) (DYNSTATS_HASHTABLE_SIZE_OVERPROVISIONING * b->maxCardinality + 1);
	if (b->table == NULL) {
		CHKmalloc(survivor_table = create_hashtable(htab_sz, hash_from_string, key_equals_string,
			no_op_free));
	}
	CHKmalloc(new_table = create_hashtable(htab_sz, hash_from_string, key_equals_string, no_op_free));
	statsobj.UnlinkAllCounters(b->stats);
	if (b->survivor_table != NULL) {
		dynstats_destroyCountersIn(b, b->survivor_table, b->survivor_ctrs);
	}
	b->survivor_table = (b->table == NULL) ? survivor_table : b->table;
	b->survivor_ctrs = b->ctrs;
	b->table = new_table;
	b->ctrs = NULL;
finalize_it:
	if (iRet != RS_RET_OK) {
		LogError(errno, RS_RET_INTERNAL_ERROR, "error trying to evict "
			"TTL-expired metrics of dyn-stats bucket named: %s", b->name);
		if (new_table == NULL) {
			LogError(errno, RS_RET_INTERNAL_ERROR, "error trying to "
				"initialize hash-table for dyn-stats bucket named: %s", b->name);
		} else {
			assert(0); /* "can" not happen -- triggers Coverity CID 184307:
			hashtable_destroy(new_table, 0);
			We keep this as guard should code above change in the future */
		}
		if (b->table == NULL) {
			if (survivor_table == NULL) {
				LogError(errno, RS_RET_INTERNAL_ERROR, "error trying to initialize "
				"ttl-survivor hash-table for dyn-stats bucket named: %s", b->name);
			} else {
				hashtable_destroy(survivor_table, 0);
			}
		}
	}
	RETiRet;
}

static rsRetVal
dynstats_resetBucket(dynstats_bucket_t *b) {
	DEFiRet;
	pthread_rwlock_wrlock(&b->lock);
	CHKiRet(dynstats_rebuildSurvivorTable(b));
	STATSCOUNTER_INC(b->ctrPurgeTriggered, b->mutCtrPurgeTriggered);
	timeoutComp(&b->metricCleanupTimeout, b->unusedMetricLife);
finalize_it:
	pthread_rwlock_unlock(&b->lock);
	RETiRet;
}

static void
dynstats_resetIfExpired(dynstats_bucket_t *b) {
	long timeout;
	pthread_rwlock_rdlock(&b->lock);
	timeout = timeoutVal(&b->metricCleanupTimeout);
	pthread_rwlock_unlock(&b->lock);
	if (timeout == 0) {
		LogMsg(0, RS_RET_TIMED_OUT, LOG_INFO, "dynstats: bucket '%s' is being reset", b->name);
		dynstats_resetBucket(b);
	}
}

static void
dynstats_readCallback(statsobj_t __attribute__((unused)) *ignore, void *b) {
	dynstats_buckets_t *bkts;
	bkts = &loadConf->dynstats_buckets;

	pthread_rwlock_rdlock(&bkts->lock);
	dynstats_resetIfExpired((dynstats_bucket_t *) b);
	pthread_rwlock_unlock(&bkts->lock);
}

static rsRetVal
dynstats_initNewBucketStats(dynstats_bucket_t *b) {
	DEFiRet;

	CHKiRet(statsobj.Construct(&b->stats));
	CHKiRet(statsobj.SetOrigin(b->stats, UCHAR_CONSTANT("dynstats.bucket")));
	CHKiRet(statsobj.SetName(b->stats, b->name));
	CHKiRet(statsobj.SetReportingNamespace(b->stats, UCHAR_CONSTANT("values")));
	statsobj.SetReadNotifier(b->stats, dynstats_readCallback, b);
	CHKiRet(statsobj.ConstructFinalize(b->stats));

finalize_it:
	RETiRet;
}

/* try to open a file which has a state file. If the state file does not
 * exist or cannot be read, an error is returned.
 */
static rsRetVal ATTR_NONNULL(1) openFileWithStateFile(dynstats_bucket_t *b) {
	DEFiRet;
	struct stat stat_buf;
	uchar statefile[MAXFNAME];
	uchar statefname[MAXFNAME];

	uchar *const statefn = getStateFileName(b, statefile, sizeof(statefile));
	assert(statefn);
	/* Get full path and file name */
	getFullStateFileName(b, statefn, statefname, sizeof(statefname));
	DBGPRINTF("opening statefile for '%s', state file '%s'\n", b->name, statefname);

	/* check if the file exists */
	if(stat((char*) statefname, &stat_buf) == -1) {
		LogMsg(0, RS_RET_FILE_NOT_FOUND, LOG_INFO,
				"dyn_stats: warning state file doesn't exist: '%s'", statefname);
		FINALIZE;
	}

	json_object *json_obj = fjson_object_from_file((const char*)statefname);
	if (!json_obj) {
		LogMsg(0, RS_RET_JSON_UNUSABLE, LOG_INFO,
				"dyn_stats: error couldn't read json from file.");
		FINALIZE;
	}

	/* expected json format
	 * { "name": "bucketname" "values": { "foo": "1" } }
	 */

	json_object *json_obj_metrics = NULL;
	if (json_object_object_get_ex(json_obj, DYNSTATS_BUCKET_PERSIST_VALUES_NAME, &json_obj_metrics)) {
		struct json_object_iterator it, itEnd;
		for (it = json_object_iter_begin(json_obj_metrics), itEnd = json_object_iter_end(json_obj_metrics);
				!json_object_iter_equal(&it, &itEnd);
				json_object_iter_next(&it)) {
			const uchar *metric = (const uchar*)json_object_iter_peek_name(&it);
			uint64_t val = json_object_get_int64(json_object_iter_peek_value(&it));
			// now add the counter to the bucket with offset
			dynstats_addNewCtr(b, metric, 0, val);
		}
	}
	json_object_put(json_obj);

finalize_it:
	RETiRet;
}

static rsRetVal
dynstats_openStateFile(dynstats_bucket_t *b) {
	DEFiRet;

	CHKiRet(openFileWithStateFile(b));

finalize_it:
	RETiRet;
}

static rsRetVal
dynstats_newBucket(const uchar* name, uint8_t resettable, uint32_t maxCardinality, uint32_t unusedMetricLife,
		uint32_t persistStateInterval, const uchar *stateFileDirectory) {
	dynstats_bucket_t *b;
	dynstats_buckets_t *bkts;
	uint8_t lock_initialized, metric_count_mutex_initialized;
	pthread_rwlockattr_t bucket_lock_attr;
	DEFiRet;

	lock_initialized = metric_count_mutex_initialized = 0;
	b = NULL;

	bkts = &loadConf->dynstats_buckets;

	if (bkts->initialized) {
		CHKmalloc(b = calloc(1, sizeof(dynstats_bucket_t)));
		b->resettable = resettable;
		b->maxCardinality = maxCardinality;
		b->unusedMetricLife = 1000 * unusedMetricLife;
		b->persistStateInterval = persistStateInterval;
		b->nUpdates = 0;
		CHKmalloc(b->name = ustrdup(name));
		if (stateFileDirectory) {
			CHKmalloc(b->stateFileDirectory = ustrdup(stateFileDirectory));
		}

		pthread_rwlockattr_init(&bucket_lock_attr);
#ifdef HAVE_PTHREAD_RWLOCKATTR_SETKIND_NP
		pthread_rwlockattr_setkind_np(&bucket_lock_attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
#endif

		pthread_rwlock_init(&b->lock, &bucket_lock_attr);
		lock_initialized = 1;
		pthread_mutex_init(&b->mutMetricCount, NULL);
		metric_count_mutex_initialized = 1;

		CHKiRet(dynstats_initNewBucketStats(b));

		CHKiRet(dynstats_resetBucket(b));

		CHKiRet(dynstats_addBucketMetrics(bkts, b, name));

		pthread_rwlock_wrlock(&bkts->lock);
		if (bkts->list == NULL) {
			bkts->list = b;
		} else {
			b->next = bkts->list;
			bkts->list = b;
		}
		pthread_rwlock_unlock(&bkts->lock);
	} else {
		LogError(0, RS_RET_INTERNAL_ERROR, "dynstats: bucket creation failed, as "
		"global-initialization of buckets was unsuccessful");
		ABORT_FINALIZE(RS_RET_INTERNAL_ERROR);
	}
finalize_it:
	if (iRet != RS_RET_OK) {
		if (metric_count_mutex_initialized) {
			pthread_mutex_destroy(&b->mutMetricCount);
		}
		if (lock_initialized) {
			pthread_rwlock_destroy(&b->lock);
		}
		if (b != NULL) {
			dynstats_destroyBucket(b);
		}
	}
	RETiRet;
}

rsRetVal
dynstats_processCnf(struct cnfobj *o) {
	struct cnfparamvals *pvals;
	short i;
	uchar *name = NULL;
	uchar *stateFileDirectory = NULL;
	uint8_t resettable = DYNSTATS_DEFAULT_RESETTABILITY;
	uint32_t maxCardinality = DYNSTATS_DEFAULT_MAX_CARDINALITY;
	uint32_t unusedMetricLife = DYNSTATS_DEFAULT_UNUSED_METRIC_LIFE;
	uint32_t persistStateInterval = DYNSTATS_DEFAULT_PERSISTSTATEINTERVAL;
	DEFiRet;

	pvals = nvlstGetParams(o->nvlst, &modpblk, NULL);
	if(pvals == NULL) {
		ABORT_FINALIZE(RS_RET_MISSING_CNFPARAMS);
	}

	for(i = 0 ; i < modpblk.nParams ; ++i) {
		if(!pvals[i].bUsed)
			continue;
		if(!strcmp(modpblk.descr[i].name, DYNSTATS_PARAM_NAME)) {
			CHKmalloc(name = (uchar*)es_str2cstr(pvals[i].val.d.estr, NULL));
		} else if (!strcmp(modpblk.descr[i].name, DYNSTATS_PARAM_RESETTABLE)) {
			resettable = (pvals[i].val.d.n != 0);
		} else if (!strcmp(modpblk.descr[i].name, DYNSTATS_PARAM_MAX_CARDINALITY)) {
			maxCardinality = (uint32_t) pvals[i].val.d.n;
		} else if (!strcmp(modpblk.descr[i].name, DYNSTATS_PARAM_UNUSED_METRIC_LIFE)) {
			unusedMetricLife = (uint32_t) pvals[i].val.d.n;
		} else if (!strcmp(modpblk.descr[i].name, DYNSTATS_PARAM_PERSISTSTATEINTERVAL)) {
			persistStateInterval = (uint32_t) pvals[i].val.d.n;
		} else if (!strcmp(modpblk.descr[i].name, DYNSTATS_PARAM_STATEFILE_DIRECTORY)) {
			CHKmalloc(stateFileDirectory = (uchar*)es_str2cstr(pvals[i].val.d.estr, NULL));
		} else {
			dbgprintf("dyn_stats: program error, non-handled "
					  "param '%s'\n", modpblk.descr[i].name);
		}
	}
	if (name != NULL) {
		CHKiRet(dynstats_newBucket(name, resettable, maxCardinality, unusedMetricLife,
					persistStateInterval, stateFileDirectory));
	}

finalize_it:
	free(name);
	free(stateFileDirectory);
	cnfparamvalsDestruct(pvals, &modpblk);
	RETiRet;
}

rsRetVal
dynstats_initCnf(dynstats_buckets_t *bkts) {
	DEFiRet;

	bkts->initialized = 0;

	bkts->list = NULL;
	CHKiRet(statsobj.Construct(&bkts->global_stats));
	CHKiRet(statsobj.SetOrigin(bkts->global_stats, UCHAR_CONSTANT("dynstats")));
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

void
dynstats_destroyAllBuckets(void) {
	dynstats_buckets_t *bkts;
	dynstats_bucket_t *b;
	bkts = &loadConf->dynstats_buckets;
	if (bkts->initialized) {
		pthread_rwlock_wrlock(&bkts->lock);
		while(1) {
			b = bkts->list;
			if (b == NULL) {
				break;
			} else {
				bkts->list = b->next;
				dynstats_destroyBucket(b);
			}
		}
		statsobj.Destruct(&bkts->global_stats);
		pthread_rwlock_unlock(&bkts->lock);
		pthread_rwlock_destroy(&bkts->lock);
	}
}

dynstats_bucket_t *
dynstats_findBucket(const uchar* name) {
	dynstats_buckets_t *bkts;
	dynstats_bucket_t *b;
	bkts = &loadConf->dynstats_buckets;
	if (bkts->initialized) {
		pthread_rwlock_rdlock(&bkts->lock);
		b = bkts->list;
		while(b != NULL) {
			if (! ustrcmp(name, b->name)) {
				break;
			}
			b = b->next;
		}

		if (b && b->persistStateInterval) {
			dynstats_openStateFile(b);
		}
		pthread_rwlock_unlock(&bkts->lock);
	} else {
		b = NULL;
		LogError(0, RS_RET_INTERNAL_ERROR, "dynstats: bucket lookup failed, as global-initialization "
		"of buckets was unsuccessful");
	}

	return b;
}

static rsRetVal
dynstats_createCtr(dynstats_bucket_t *b, const uchar* metric, dynstats_ctr_t **ctr) {
	DEFiRet;

	CHKmalloc(*ctr = calloc(1, sizeof(dynstats_ctr_t)));
	CHKmalloc((*ctr)->metric = ustrdup(metric));
	STATSCOUNTER_INIT((*ctr)->ctr, (*ctr)->mutCtr);
	CHKiRet(statsobj.AddManagedCounter(b->stats, metric, ctrType_IntCtr,
				b->resettable ? CTR_FLAG_MUST_RESET : CTR_FLAG_NONE,
				&(*ctr)->ctr, &(*ctr)->pCtr, 0));
finalize_it:
	if (iRet != RS_RET_OK) {
		if ((*ctr) != NULL) {
			free((*ctr)->metric);
			free(*ctr);
			*ctr = NULL;
		}
	}
	RETiRet;
}

/* not supported in older platforms - disable */
#ifndef O_CLOEXEC
#define O_CLOEXEC 0
#endif
/*
 *	Helper function adapted from imfile of the same name - possible candidate for refactor.
 */
static rsRetVal ATTR_NONNULL()
atomicWriteStateFile(const char *fn, const char *content) {
	DEFiRet;
	const int fd = open(fn, O_CLOEXEC | O_NOCTTY | O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if(fd < 0) {
		LogError(errno, RS_RET_IO_ERROR, "dynstats: cannot open state file '%s' for "
			"persisting file state - some data will probably be duplicated "
			"on next startup", fn);
		ABORT_FINALIZE(RS_RET_IO_ERROR);
	}

	const size_t toWrite = strlen(content);
	const ssize_t w = write(fd, content, toWrite);
	if(w != (ssize_t) toWrite) {
		LogError(errno, RS_RET_IO_ERROR, "dynstats: partial write to state file '%s' "
			"this may cause trouble in the future. We will try to delete the "
			"state file, as this provides most consistent state", fn);
		unlink(fn);
		ABORT_FINALIZE(RS_RET_IO_ERROR);
	}

finalize_it:
	if(fd >= 0) {
		close(fd);
	}
	RETiRet;
}

static const uchar *
getStateFileDir(dynstats_bucket_t *b)
{
	const uchar *wrkdir;
	assert(b != NULL);
	if (b->stateFileDirectory == NULL) {
		wrkdir = glblGetWorkDirRaw();
	} else {
		wrkdir = b->stateFileDirectory;
	}
	return(wrkdir);
}

/*
*	Helper function to combine statefile and workdir
*/
static int getFullStateFileName(dynstats_bucket_t *b, uchar* pszstatefile, uchar* pszout, int buflen)
{
	int lenout;
	const uchar* pszworkdir;

	/* Get Raw Workdir, if it is NULL we need to propper handle it */
	pszworkdir = getStateFileDir(b);

	/* Construct file name */
	lenout = snprintf((char*)pszout, buflen, "%s/%s",
					(char*) (pszworkdir == NULL ? "." : (char*) pszworkdir), (char*)pszstatefile);

	/* return out length */
	return lenout;
}

/* this generates a state file name suitable for the given file. To avoid
 * malloc calls, it must be passed a buffer which should be MAXFNAME large.
 * Note: the buffer is not necessarily populated ... always ONLY use the
 * RETURN VALUE!
 * This function is guranteed to work only on config data and DOES NOT
 * open or otherwise modify disk file state.
 */
static uchar * ATTR_NONNULL(1, 2)
getStateFileName(const dynstats_bucket_t *const pbucket,
		uchar *const __restrict__ buf,
		const size_t lenbuf) {

	DBGPRINTF("getStateFileName for '%s'\n", pbucket->name);
	snprintf((char*)buf, lenbuf - 1, "dynstats-state:%s", pbucket->name);
	DBGPRINTF("getStateFileName:  state file name now is %s\n", buf);
	return buf;
}

/* This function persists dynstats_bucket_t data, which for the time being:
 * metric bucket counters
 */
static rsRetVal ATTR_NONNULL(1)
persistBucketState(dynstats_bucket_t *b) {
	DEFiRet;
	uchar statefile[MAXFNAME];
	uchar statefname[MAXFNAME];
	struct json_object *jval = NULL;
	struct json_object *json = NULL;
	struct json_object *json_bucket_values = NULL;

	assert (b->name);
	uchar *const statefn = getStateFileName(b, statefile, sizeof(statefile));
	/* Get full path and file name */
	getFullStateFileName(b, statefn, statefname, sizeof(statefname));
	DBGPRINTF("persisting state for '%s', state file '%s'\n", b->name, statefname);

	CHKmalloc(json = json_object_new_object());
	jval = json_object_new_string((char*) b->name);
	json_object_object_add(json, DYNSTATS_BUCKET_PERSIST_NAME, jval);

	// walk the hashtable and create a val for each
	CHKmalloc(json_bucket_values = json_object_new_object());
	json_object_object_add(json, DYNSTATS_BUCKET_PERSIST_VALUES_NAME, json_bucket_values);

	pthread_rwlock_rdlock(&b->lock);
	if (hashtable_count(b->table) > 0) {
		struct hashtable_itr *itr = hashtable_iterator(b->table);
		dynstats_ctr_t *pctr = NULL;
		do {
			pctr = (dynstats_ctr_t*) hashtable_iterator_value(itr);
			jval = json_object_new_int64(pctr->ctr);
			json_object_object_add(json_bucket_values, (const char*)pctr->metric, jval);
		} while (hashtable_iterator_advance(itr));
		free(itr);
	}
	pthread_rwlock_unlock(&b->lock);

	const char *jstr =  json_object_to_json_string_ext(json, JSON_C_TO_STRING_SPACED);
	CHKiRet(atomicWriteStateFile((const char*)statefname, jstr));

finalize_it:
	if(iRet != RS_RET_OK) {
		LogError(0, iRet, "dynstats: could not persist state "
				"file %s - data may be repeated on next "
				"startup. Is WorkDirectory set?",
				statefname);
	}
	json_object_put(json);

	RETiRet;
}

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized" /* TODO: how can we fix these warnings? */
#endif
static rsRetVal
dynstats_addNewCtr(dynstats_bucket_t *b, const uchar* metric, uint8_t doInitialIncrement, uint64_t doInitialOffset) {
	dynstats_ctr_t *ctr;
	dynstats_ctr_t *found_ctr, *survivor_ctr, *effective_ctr;
	int created;
	uchar *copy_of_key = NULL;
	DEFiRet;

	created = 0;
	ctr = NULL;

	if ((unsigned) ATOMIC_FETCH_32BIT_unsigned(&b->metricCount, &b->mutMetricCount) >= b->maxCardinality) {
		ABORT_FINALIZE(RS_RET_OUT_OF_MEMORY);
	}

	CHKiRet(dynstats_createCtr(b, metric, &ctr));

	pthread_rwlock_wrlock(&b->lock);
	found_ctr = (dynstats_ctr_t*) hashtable_search(b->table, ctr->metric);
	if (found_ctr != NULL) {
		if (doInitialIncrement) {
			STATSCOUNTER_INC(found_ctr->ctr, found_ctr->mutCtr);
		}
	} else {
		copy_of_key = ustrdup(ctr->metric);
		if (copy_of_key != NULL) {
			survivor_ctr = (dynstats_ctr_t*) hashtable_search(b->survivor_table, ctr->metric);
			if (survivor_ctr == NULL) {
				effective_ctr = ctr;
			} else {
				effective_ctr = survivor_ctr;
				if (survivor_ctr->prev != NULL) {
					survivor_ctr->prev->next = survivor_ctr->next;
				}
				if (survivor_ctr->next != NULL) {
					survivor_ctr->next->prev = survivor_ctr->prev;
				}
				if (survivor_ctr == b->survivor_ctrs) {
					b->survivor_ctrs = survivor_ctr->next;
				}
			}
			if ((created = hashtable_insert(b->table, copy_of_key, effective_ctr))) {
				statsobj.AddPreCreatedCtr(b->stats, effective_ctr->pCtr);
			}
		}
		if (created) {
			if (b->ctrs != NULL) {
				b->ctrs->prev = effective_ctr;
			}
			effective_ctr->prev = NULL;
			effective_ctr->next = b->ctrs;
			b->ctrs = effective_ctr;
			if (doInitialIncrement) {
				STATSCOUNTER_INC(effective_ctr->ctr, effective_ctr->mutCtr);
			}
			if (doInitialOffset) {
				// doInitialOffset regardless of the state of GatherStats
				ATOMIC_ADD_uint64(&effective_ctr->ctr, &effective_ctr->mutCtr, doInitialOffset);
			}
		}
	}
	pthread_rwlock_unlock(&b->lock);

	if (found_ctr != NULL) {
		//ignore
	} else if (created && (effective_ctr != survivor_ctr)) {
		ATOMIC_INC(&b->metricCount, &b->mutMetricCount);
		STATSCOUNTER_INC(b->ctrNewMetricAdd, b->mutCtrNewMetricAdd);
	} else if (! created) {
		if (copy_of_key != NULL) {
			free(copy_of_key);
		}
		ABORT_FINALIZE(RS_RET_OUT_OF_MEMORY);
	}

finalize_it:
	if (((! created) || (effective_ctr != ctr)) && (ctr != NULL)) {
		dynstats_destroyCtr(ctr);
	}
	RETiRet;
}
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic pop
#endif

rsRetVal
dynstats_inc(dynstats_bucket_t *b, uchar* metric) {
	dynstats_ctr_t *ctr = NULL;
	DEFiRet;

	if (! GatherStats) {
		FINALIZE;
	}

	if (ustrlen(metric) == 0) {
		STATSCOUNTER_INC(b->ctrNoMetric, b->mutCtrNoMetric);
		FINALIZE;
	}

	if (pthread_rwlock_tryrdlock(&b->lock) == 0) {
		ctr = (dynstats_ctr_t *) hashtable_search(b->table, metric);
		if (ctr != NULL) {
			STATSCOUNTER_INC(ctr->ctr, ctr->mutCtr);
		}
		pthread_rwlock_unlock(&b->lock);
	} else {
		ABORT_FINALIZE(RS_RET_NOENTRY);
	}

	if (ctr == NULL) {
		CHKiRet(dynstats_addNewCtr(b, metric, 1, 0));
	}
finalize_it:
	if (iRet != RS_RET_OK) {
		if (iRet == RS_RET_NOENTRY) {
			/* NOTE: this is not tested (because it requires very strong orchestration to
			guarantee contended lock for testing) */
			STATSCOUNTER_INC(b->ctrOpsIgnored, b->mutCtrOpsIgnored);
		} else {
			STATSCOUNTER_INC(b->ctrOpsOverflow, b->mutCtrOpsOverflow);
		}
	}

	if(b->persistStateInterval > 0 && ++b->nUpdates >= b->persistStateInterval) {
		persistBucketState(b);
		b->nUpdates = 0;
	}
	RETiRet;
}


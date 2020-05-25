#!/bin/bash

. ${srcdir:=.}/diag.sh init
generate_conf
add_conf '
ruleset(name="stats") {
  action(type="omfile" file="'${RSYSLOG_DYNNAME}'.out.stats.log")
}

module(load="../plugins/impstats/.libs/impstats" interval="1" severity="7" resetCounters="on" Ruleset="stats" bracketing="on")
template(name="outfmt" type="string" string="%$.timestamp% %msg%  val=%$.val%\n")

perctile_stats(name="msg_stats" percentiles=["95", "50"] windowsize="1000")

# test with a small window
set $.val = field($msg, 58, 2);

# do a test observe here.
set $.status = perctile_observe("msg_stats", "testkey", $.val);

action(type="omfile" file=`echo $RSYSLOG_OUT_LOG` template="outfmt")
'
startup
injectmsg 1 1000
wait_for_stats_flush ${RSYSLOG_DYNNAME}.out.stats.log
rst_msleep 1100 # wait for stats flush
#custom_content_check 'msg_stats_testkey_p95=950' "${RSYSLOG_DYNNAME}.out.stats.log"
#custom_content_check 'msg_stats_testkey_p50=500' "${RSYSLOG_DYNNAME}.out.stats.log"

injectmsg 1001 1000
wait_for_stats_flush ${RSYSLOG_DYNNAME}.out.stats.log
wait_queueempty
rst_msleep 1100 # wait for stats flush
wait_for_stats_flush ${RSYSLOG_DYNNAME}.out.stats.log

custom_content_check 'msg_stats_testkey_p95=950' "${RSYSLOG_DYNNAME}.out.stats.log"
custom_content_check 'msg_stats_testkey_p50=500' "${RSYSLOG_DYNNAME}.out.stats.log"

custom_content_check 'msg_stats_testkey_p95=1950' "${RSYSLOG_DYNNAME}.out.stats.log"
custom_content_check 'msg_stats_testkey_p50=1500' "${RSYSLOG_DYNNAME}.out.stats.log"

echo doing shutdown
shutdown_when_empty
echo wait on shutdown
wait_shutdown
exit_test

#!/bin/bash

. ${srcdir:=.}/diag.sh init
generate_conf
add_conf '
ruleset(name="stats") {
  action(type="omfile" file="'${RSYSLOG_DYNNAME}'.out.stats.log")
}

module(load="../plugins/impstats/.libs/impstats" interval="1" severity="7" resetCounters="on" Ruleset="stats" bracketing="on")
#template(name="outfmt" type="string" string="%msg% %$.increment_successful%\n")
template(name="outfmt" type="string" string="%$.timestamp% %msg%  val=%$.val%\n")
template(name="timestamp" type="string" string="%$.ts% %msg%  %$.epochtime% %$.millis% %$.cur_epoch% %$.delta_secs% %$.delta_millis%\n")

# for now, we only check if type is set to something
#dyn_stats(name="msg_stats")
dyn_stats(name="msg_stats" type="yes" percentiles=["95", "50"] windowsize="1000" )

# test with a small window
#set $.msg_prefix = field($msg, 8, 1);
set $.val = field($msg, 58, 2);

#set $.increment_successful = dyn_inc("msg_stats", $.msg_prefix);

# do a test observe here.
#set $.status = dyn_perctile_observe("msg_stats", "testkey", 30000000);
set $.status = dyn_perctile_observe("msg_stats", "testkey", $.val);

action(type="omfile" file=`echo $RSYSLOG_OUT_LOG` template="outfmt")

## get millisecond granularity from timestamp
#
#set $.ts = "20200305190618.986";
#set $.ts_conv = "2020-03-05T19:06:18.986Z";
#set $.epochtime = parse_time($.ts_conv);
#set $.millis = field($.ts, 46, 2);
#
#set $.ts_epoch = "2020-03-07T19:06:18.986Z";
#set $.cur_epoch = parse_time($.ts_epoch);
#
#set $.delta_secs = 1000*($.cur_epoch - $.epochtime);
#set $.delta_millis = 1000*($.cur_epoch - $.epochtime) - $.millis;
#
#action(type="omfile" file="'${RSYSLOG_DYNNAME}'.ts" template="timestamp")
' 
startup
injectmsg 1 1000
wait_for_stats_flush ${RSYSLOG_DYNNAME}.out.stats.log
rst_msleep 1100 # wait for stats flush
#custom_content_check 'msg_stats_testkey_p95=950' "${RSYSLOG_DYNNAME}.out.stats.log"
#custom_content_check 'msg_stats_testkey_p50=500' "${RSYSLOG_DYNNAME}.out.stats.log"

injectmsg 1001 1000
#wait_for_stats_flush ${RSYSLOG_DYNNAME}.out.stats.log
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
#exit_test

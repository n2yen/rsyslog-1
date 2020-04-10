#!/bin/bash

. ${srcdir:=.}/diag.sh init
generate_conf
add_conf '
ruleset(name="stats") {
  action(type="omfile" file="'${RSYSLOG_DYNNAME}'.out.stats.log")
}

module(load="../plugins/imfile/.libs/imfile")
input(type="imfile" File="./testsuites/core-test.log" tag="file:")
module(load="../plugins/impstats/.libs/impstats" interval="1" severity="7" resetCounters="on" Ruleset="stats" bracketing="on")
#template(name="outfmt" type="string" string="%msg%\n")
#template(name="timestamp" type="string" string="%$.ts% %msg%  %$.ts_parse% %$.ts_epochtime% \n")
template(name="timestamp" type="string" string="%$.ts% %msg%  %$.ts_parse% %$.ts_epochtime% %$.millis% %.delta_millis%\n")

# for now, we only check if type is set to something
#dyn_stats(name="core-lag" type="yes" percentiles=["95", "50"] windowsize="32000" )

# extract timestamp using tilde
set $.ts = field($msg, 96, 2);

if $.ts startswith "***FIELD NOT FOUND" then {
	stop
}

# convert to rsyslog usable time.
# 20171103184919.875 --> 2017-11-03T18:49:19.875Z
set $.ts_parse = substring($.ts, 0, 4) & "-" & substring($.ts, 4, 2) & "-" & substring($.ts, 6, 2) & "T"
									& substring($.ts, 8, 2) & ":" & substring($.ts, 10, 2) & ":" & substring($.ts, 12, 2) & "Z";
set $.ts_epochtime = parse_time($.ts_parse);

set $.ts_cur_epoch = "2020-04-01T10:06:18.986Z";
set $.ts_cur_epoch = parse_time($.ts_cur_epoch);
set $.millis = field($.ts, 46, 2);
set $.delta_millis = 1000*($.ts_cur_epoch - $.ts_epochtime) - $.millis;
#action(type="omfile" file="'${RSYSLOG_DYNNAME}'.ts" template="timestamp")
action(type="omfile" file="'${RSYSLOG_DYNNAME}'.ts" template="outfmt")

# do a test observe here.
#set $.status = dyn_perctile_observe("core-lag", "core-test.log", $.delta_millis);
'

startup
wait_for_stats_flush ${RSYSLOG_DYNNAME}.out.stats.log
#injectmsg_file $srcdir/testsuites/core-test.log
wait_queueempty
rst_msleep 1100 # wait for stats flush
wait_for_stats_flush ${RSYSLOG_DYNNAME}.out.stats.log
# eventually we'd want some real p95 test.
#custom_content_check 'msg_stats_testkey_p95=950' "${RSYSLOG_DYNNAME}.out.stats.log"
#custom_content_check 'msg_stats_testkey_p50=500' "${RSYSLOG_DYNNAME}.out.stats.log"
#
#custom_content_check 'msg_stats_testkey_p95=1950' "${RSYSLOG_DYNNAME}.out.stats.log"
#custom_content_check 'msg_stats_testkey_p50=1500' "${RSYSLOG_DYNNAME}.out.stats.log"
echo doing shutdown
shutdown_when_empty
echo wait on shutdown
wait_shutdown
#exit_test

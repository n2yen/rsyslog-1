#!/bin/bash

. ${srcdir:=.}/diag.sh init
generate_conf
add_conf '
template(name="timestamp" type="string" string="%$.ts% %msg%  %$.epochtime% %$.millis% %$.cur_epoch% %$.delta_secs% %$.delta_millis%\n")

# get millisecond granularity from timestamp
set $.ts = "20200305190618.986";
set $.ts_conv = "2020-03-05T19:06:18.986Z";
set $.epochtime = parse_time($.ts_conv);
set $.millis = field($.ts, 46, 2);

set $.ts_epoch = "2020-03-07T19:06:18.986Z";
set $.cur_epoch = parse_time($.ts_epoch);

set $.delta_secs = 1000*($.cur_epoch - $.epochtime);
set $.delta_millis = 1000*($.cur_epoch - $.epochtime) - $.millis;

action(type="omfile" file="'${RSYSLOG_DYNNAME}'.ts" template="timestamp")
' 
startup
injectmsg 1 10
wait_queueempty
echo doing shutdown
shutdown_when_empty
echo wait on shutdown
wait_shutdown
#exit_test

#!/bin/bash
# released under ASL 2.0
. ${srcdir:=.}/diag.sh init
generate_conf
add_conf '
module(load="../plugins/imtcp/.libs/imtcp")
input(type="imtcp" port="0" listenPortFileName="'$RSYSLOG_DYNNAME'.tcpflood_port")
template(name="outfmt" type="string" string="%$!%\n")
template(name="outlocal" type="string" string="%$.%\n")

local4.* {
  set $.ok = parse_json("{\"offsets\": [ { \"a\": 9, \"b\": 0, \"c\": \"boo\", \"d\": null },\
                                         { \"a\": 9, \"b\": 3, \"c\": null, \"d\": null } ],\
                                         \"foo\": 3, \"bar\": 28 }\"", "\$!parsed");
	if $.ret == 0 then {
		set $.val = json_array_length("\$!parsed!offsets");
		action(type="omfile" file=`echo $RSYSLOG_OUT_LOG` template="outfmt")
		action(type="omfile" file=`echo $RSYSLOG_OUT_LOG` template="outlocal")
	}
}
'

startup
tcpflood -m1
shutdown_when_empty
wait_shutdown
content_count_check '{ "parsed": { "offsets": [ { "a": 9, "b": 0, "c": "boo", "d": null }, { "a": 9, "b": 3, "c": null, "d": null } ], "foo": 3, "bar": 28 }' 1
content_count_check '{ "ok": 0, "val": 2 }' 1
exit_test

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
  set $.ret = parse_json("{\"offsets\": [ { \"a\": 9, \"b\": 0, \"c\": \"boo\", \"d\": null },\
                                         { \"a\": 9, \"b\": 3, \"c\": null, \"d\": null } ],\
                                         \"booltest\": true,\
                                         \"int64\": 1234567890,\
                                         \"nulltest\": null,\
                                         \"double\": 12345.67890,\
                                         \"foo\": 3, \"bar\": 28 }\"", "\$!parsed");
	if $.ret == 0 then {
		set $!foo!bar = 3;
		set $.index = 1;
		set $.index = "1";
		set $.test = "a";
		set $.res1 = get_property_by_dynkey($!parsed!offsets, $.index);
		set $.res2 = get_property_by_dynkey($!parsed!offsets[1], $.test);
		reset $.test = "bar";
		set $.res3 = get_property_by_dynkey($!foo, $.test);
		reset $.index = 5;
		set $.res4 = get_property_by_dynkey($!parsed!offsets, $.index);
		set $.key = "test";
		set $.res5 = get_property_by_dynkey($., $.key);
		reset $.key = "foo";
		set $.res6 = get_property_by_dynkey($!, $.key);

		# for static keys
		set $.res7 = get_property_by_dynkey($!foo, "bar");

		reset $.key = "ar";
		set $.res8 = get_property_by_dynkey($!foo, "b" & $.key);

		set $.res9 = get_property_by_dynkey($!foo!bar, "");

		reset $.key = "";
		set $.res10 = get_property_by_dynkey($!foo!bar, $.key);

		set $.res11 = get_property_by_dynkey($!parsed!booltest, "");

		reset $.key = "int64";
		set $.res12 = get_property_by_dynkey($!parsed, $.key);
		reset $.key = "nulltest";
		set $.res13 = get_property_by_dynkey($!parsed, $.key);
		reset $.key = "double";
		set $.res14 = get_property_by_dynkey($!parsed, $.key);

		#set $.res16 = get_property_by_dynkey($, "msg");

		action(type="omfile" file=`echo $RSYSLOG_OUT_LOG` template="outfmt")
		action(type="omfile" file=`echo $RSYSLOG_OUT_LOG` template="outlocal")
	}
}
'

startup
tcpflood -m1
shutdown_when_empty
wait_shutdown
# check message property content "$!"
content_count_check '{ "parsed": { "offsets": [ { "a": 9, "b": 0, "c": "boo", "d": null }, { "a": 9, "b": 3, "c": null, "d": null } ], "foo": 3, "bar": 28 }, "foo": { "bar": 3 } }' 1
# check local variable content "$."
content_count_check '{ "ret": 0, "index": 5, "test": "bar", "res1": { "a": 9, "b": 3, "c": null, "d": null }, "res2": 9, "res3": 3, "res4": "", "key": "foo", "res5": "bar", "res6": { "bar": 3 } }' 1
exit_test

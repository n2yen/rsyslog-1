#!/bin/bash
# released under ASL 2.0
. ${srcdir:=.}/diag.sh init
generate_conf
add_conf '
module(load="../plugins/imtcp/.libs/imtcp")
input(type="imtcp" port="0" listenPortFileName="'$RSYSLOG_DYNNAME'.tcpflood_port")
template(name="outfmt" type="string" string="%$!%\n")
template(name="outlocal" type="string" string="%$.%\n")
template(name="outlocal2" type="string" string="out2=%$.%\n")
template(name="test_get_property_by_key" type="list") {
  constant(value="get_property_by_key() = ")
  property(name="$.res")
  constant(value="\n")
}
template(name="test_get_property_by_key2" type="list") {
  constant(value="test_get_property_by_key2() = ")
  property(name="$.res2")
  constant(value="\n")
}

template(name="test_get_property_by_key3" type="list") {
  constant(value="test_get_property_by_key3() = ")
  property(name="$.res3")
  constant(value="\n")
}
#template(name="extract" type="string" string="%$!parsed!offsets[$.index]!$.test%")
template(name="extract" type="string" string="%$!parsed!offsets[\"$.index\"]%")
#template(name="extract" type="string" string="%$!parsed!offsets[1]%")
#template(name="extract" type="string" string="%$!parsed!offsets%")

local4.* {
  set $.ret = parse_json("{\"offsets\": [ { \"a\": 9, \"b\": 0, \"c\": \"boo\", \"d\": null },\
                                         { \"a\": 9, \"b\": 3, \"c\": null, \"d\": null } ],\
                                         \"foo\": 3, \"bar\": 28 }\"", "\$!parsed");
	if $.ret == 0 then {
		# do a quick test of array get
		#set $.res = get_property_by_key("\$!parsed!offsets", 1);
		#set $.res = get_property_by_key("\$!parsed!offsets[1]", 2);

		set $!foo!bar = 3;
		set $.index = 1;
		set $.test = "a";
		##set $.res = get_property_by_key("\$!parsed!offsets[\$.index]", 2);
		##set $.res = get_property_by_key("\$!parsed!offsets[\$.index]!a", 2);
		##set $.res = get_property_by_key("\$!parsed!offsets[\$.index]!\$.test", 2);
		##set $.res = exec_template("extract");
		set $.res = get_property_by_key("\$!parsed!offsets", "\$.index");
		set $.res2 = get_property_by_key("\$!parsed!offsets[1]", "\$.test");
		reset $.test = "bar";
		set $.res3 = get_property_by_key("\$!foo", "\$.test");
		reset $.index = 5;
		set $.res4 = get_property_by_key("\$!parsed!offsets", "\$.index");
		set $.key = "test";
		set $.res5 = get_property_by_key("\$.", "\$.key");
		reset $.key = "foo";
		set $.res6 = get_property_by_key("\$!", "\$.key");
		action(type="omfile" file=`echo $RSYSLOG_OUT_LOG` template="test_get_property_by_key")
		action(type="omfile" file=`echo $RSYSLOG_OUT_LOG` template="test_get_property_by_key2")
		action(type="omfile" file=`echo $RSYSLOG_OUT_LOG` template="test_get_property_by_key3")
		action(type="omfile" file=`echo $RSYSLOG_OUT_LOG` template="outlocal")
	}
}
'

startup
tcpflood -m1
shutdown_when_empty
wait_shutdown
#content_count_check '{ "parsed": { "offsets": [ { "a": 9, "b": 0, "c": "boo", "d": null }, { "a": 9, "b": 3, "c": null, "d": null } ], "foo": 3, "bar": 28 }' 1
content_count_check '{ "ret": 0, "val": 2, "res": { "a": 9, "b": 3, "c": null, "d": null } }' 1
exit_test

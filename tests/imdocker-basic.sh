#!/bin/bash
# This is part of the rsyslog testbench, licensed under ASL 2.0

# imdocker unit tests are enabled with --enable-imdocker-tests
. ${srcdir:=.}/diag.sh init

#if [ -n "${USE_GDB:-}" ] ; then
#	echo attach gdb here
#	sleep 54321 || :
#fi

generate_conf
add_conf '
template(name="template_msg_only" type="string" string="%msg%\n")
module(load="../contrib/imdocker/.libs/imdocker" PollingInterval="1"
        GetContainerLogOptions="timestamps=0&follow=1&stdout=1&stderr=0")
action(type="omfile" template="template_msg_only"  file="'$RSYSLOG_OUT_LOG'")
'

startup
NUM_ITEMS=1000
# launch a docker runtime to generate some logs.
docker run \
   --rm \
   -e num_items=$NUM_ITEMS \
   alpine \
   /bin/sh -c 'for i in `seq 1 $num_items`; do echo "log item $i"; sleep .01; done' > /dev/null

content_check_with_count 'log item' $NUM_ITEMS
echo "file name: $RSYSLOG_OUT_LOG"
exit_test


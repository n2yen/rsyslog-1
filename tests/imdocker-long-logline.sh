#!/bin/bash
# This is part of the rsyslog testbench, licensed under ASL 2.0

# imdocker unit tests are enabled with --enable-imdocker-tests
. ${srcdir:=.}/diag.sh init
generate_conf
add_conf '
template(name="template_msg_only" type="string" string="%msg%\n")
module(load="../contrib/imdocker/.libs/imdocker" PollingInterval="1"
        GetContainerLogOptions="timestamps=0&follow=1&stdout=1&stderr=0")
action(type="omfile" template="template_msg_only"  file="'$RSYSLOG_OUT_LOG'")

$MaxMessageSize 64k
'
startup

SIZE=17000
# launch container with a long log line
docker run \
  -e size=$SIZE \
  --rm \
  alpine /bin/sh -c 'printf "$(yes a | head -n $size | tr -d "\n")"; sleep 3' > /dev/null

shutdown_when_empty

# check the log line length
echo "file name: $RSYSLOG_OUT_LOG"
count=$(cat $RSYSLOG_OUT_LOG | grep 'aaaaaaa' | tr -d "\n" | wc -c)

if [ "x$count" == "x$SIZE" ]; then
  echo "correct log line length: $count"
else
  echo "Incorrect log line length - found $count, expected: $SIZE"
  error_exit 1
fi

exit_test

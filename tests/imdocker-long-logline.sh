#!/bin/bash
# This is part of the rsyslog testbench, licensed under ASL 2.0

# imdocker unit tests are enabled with --enable-imdocker-tests
. ${srcdir:=.}/diag.sh init
export COOKIE=$(tr -dc 'a-zA-Z0-9' < /dev/urandom | fold -w 10 | head -n 1)

generate_conf
add_conf '
template(name="outfmt" type="string" string="%msg%\n")

module(load="../contrib/imdocker/.libs/imdocker" PollingInterval="1"
        GetContainerLogOptions="timestamps=0&follow=1&stdout=1&stderr=0")

if $!metadata!Names == "'$COOKIE'" then {
  action(type="omfile" template="outfmt"  file="'$RSYSLOG_OUT_LOG'")
}

$MaxMessageSize 64k
'
startup

SIZE=17000
# launch container with a long log line
docker run \
  --name $COOKIE \
  -e size=$SIZE \
  --rm \
  alpine /bin/sh -c 'sleep 5; echo "$(yes a | head -n $size | tr -d "\n")"; sleep 1;' > /dev/null

shutdown_when_empty

# check the log line length
echo "file name: $COOKIE"
count=$(grep "aaaaaaa" $RSYSLOG_OUT_LOG | tr -d "\n" | wc -c)

if [ "x$count" == "x$SIZE" ]; then
  echo "correct log line length: $count"
else
  echo "Incorrect log line length - found $count, expected: $SIZE"
  error_exit 1
fi

exit_test

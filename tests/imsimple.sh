. ${srcdir:=.}/diag.sh init

export RSYSLOG_DEBUG="Debug"
USE_GDB=1

generate_conf
add_conf '
template(name="outfmt" type="string" string="%msg%\n")
module(load="../go-api/imsimple/imsimple" )
'
startup
if [ -n "${USE_GDB:-}" ] ; then
	echo attach gdb here
	sleep 54321 || :
fi
NUMMESSAGES=1
sleep 5
shutdown_when_empty
echo "file name: $RSYSLOG_OUT_LOG"
cat $RSYSLOG_OUT_LOG
#exit_test
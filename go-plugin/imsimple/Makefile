# simple build file
top_srcdir=/home/nyen/Development/GitHub/rsyslog
ROOT_DIR=${top_srcdir}

#RSRT_CFLAGS = -I/usr/include $(RSRT_CFLAGS1) $(LIBESTR_CFLAGS) $(LIBFASTJSON_CFLAGS) $(LIBSYSTEMD_CFLAGS) -W -Wall -Wformat-security -Wshadow -Wcast-align -Wpointer-arith -Wmissing-format-attribute -Werror=implicit-function-declaration -g 
#RSRT_CFLAGS = $(RSRT_CFLAGS1) $(LIBESTR_CFLAGS) $(LIBFASTJSON_CFLAGS) $(LIBSYSTEMD_CFLAGS) -W -Wall -Wformat-security -Wshadow -Wcast-align -Wpointer-arith -Wmissing-format-attribute -Werror=implicit-function-declaration -g 
#RSRT_CFLAGS1 = -I$(top_srcdir)/runtime -I$(top_srcdir) -I$(top_srcdir)/grammar

LIBESTR_CFLAGS = -I/usr/local/Cellar/libestr/0.1.11/include
LIBFASTJSON_LIBS = -L/usr/local/lib -lfastjson
LIBESTR_CFLAGS = -I/usr/local/Cellar/libestr/0.1.11/include
LIBESTR_LIBS = -L/usr/local/Cellar/libestr/0.1.11/lib -lestr

RSRT_LIBS = $(RSRT_LIBS1) $(LIBESTR_LIBS) $(LIBFASTJSON_LIBS) $(LIBSYSTEMD_LIBS) $(RSRT_TOOLS)
RSRT_LIBS1 = -L$(top_srcdir)/runtime/.libs/ -L$(top_srcdir)/grammar/.libs/ 
#RSRT_LIBS1 = -L$(top_srcdir)/runtime/.libs/ -lrsyslog -L$(top_srcdir)/grammar/.libs/ -lgrammar
RSRT_TOOLS = -L$(top_srcdir)/tools 

INCLUDES=-I${ROOT_DIR} \
 -I/usr/include \
 -I/usr/include/libfastjson \
 -I${ROOT_DIR}/runtime \
 -I${ROOT_DIR}/grammar \
 	-I/usr/local/include/libfastjson/

CFLAGS="${INCLUDES} -Doff64_t=__off64_t"
LDFLAGS="${RSRT_LIBS}"
#LDFLAGS="-module -avoid-version ${RSRT_LIBS}"
#LDFLAGS="-shared -L$(top_srcdir)/runtime/.libs/ -lrsyslog"
all: share
	gcc hello.c -o hello
	#CGO_CFLAGS=${CFLAGS} go build

share:
	CGO_CFLAGS=${CFLAGS} CGO_LDFLAGS=${LDFLAGS} go build -v -o imsimple.so -buildmode=c-shared imsimple.go
	#CGO_CFLAGS=${CFLAGS} go build -o imsimple.so -buildmode=c-shared test.go

clean:
	rm imsimple imsimple.so imsimple.h

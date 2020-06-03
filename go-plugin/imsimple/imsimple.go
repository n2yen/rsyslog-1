package main

/*
// #cgo LDFLAGS: -Wl,--unresolved-symbols=ignore-in-object-files -Wl,-allow-shlib-undefined
#cgo linux LDFLAGS: -ldl -Wl,--unresolved-symbols=ignore-in-object-files
#cgo darwin LDFLAGS: -ldl -Wl,-undefined,dynamic_lookup


#include "config.h"
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <stdarg.h>
#include <ctype.h>
#include <netinet/in.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>

// rsyslog specific includes
#include "rsyslog.h"
#include "cfsysline.h"		// access to config file objects
#include "module-template.h"
#include "ruleset.h"
#include "unicode-helper.h"
#include "rsyslog.h"
#include "errmsg.h"
#include "statsobj.h"
#include "modules.h"
#include "msg.h"

#include "dirty.h"

MODULE_TYPE_INPUT
MODULE_TYPE_NOKEEP
MODULE_CNFNAME("imsimple")

// static data
DEF_OMOD_STATIC_DATA
DEFobjCurrIf(glbl)
DEFobjCurrIf(prop)
DEFobjCurrIf(ruleset)
DEFobjCurrIf(statsobj)


struct modConfData_s {
	rsconf_t *pConf;
	instanceConf_t *root, *tail;
};


struct instanceConf_s {
	uchar *pszBindRuleset;
	uchar *pszEndpoint;
	ruleset_t *pBindRuleset;
	struct instanceConf_s *next;
};

static modConfData_t *loadModConf = NULL;
static modConfData_t *runModConf = NULL;

static rsRetVal
enqMsg(instanceConf_t *const __restrict__ inst,
			 char *const __restrict__ msg, size_t len)
			 //uchar *const __restrict__ msg, size_t len, uchar *pszTag)
{
	smsg_t *pMsg;
	DEFiRet;
	uchar *pszTag = "sometag";

	assert(msg != NULL);
	assert(pszTag != NULL);

	printf("imsimple: enqMsg - before dbgOutputTID() \n");
	dbgprintf("dbgprintf: hello from enqMsg\n");
	DBGPRINTF("dbgprintf: hello from enqMsg\n");

	// test dbg outputTID
	uchar thrdName[32];
	strcpy((char*)thrdName, "imsimple");
	dbgOutputTID((char*)thrdName);

	printf("imsimple: enqMsg - before msgConstruct() \n");
	CHKiRet(msgConstruct(&pMsg));
	printf("After msg Construct from enqMsg\n");
	dbgprintf("dbgprintf: hello from enqMsg\n");
	// MsgSetFlowControlType(pMsg, eFLOWCTL_NO_DELAY); // eFLOWCTL_LIGHT_DELAY, if we support flow control
	// // 2nd param is a prop_t type (not a string)
	// //MsgSetInputName(pMsg, "imhttp");
	// MsgSetRawMsg(pMsg, (char*)msg, len);
	// MsgSetMSGoffs(pMsg, 0);	// we do not have a header...
	// MsgSetRcvFrom(pMsg, glbl.GetLocalHostNameProp());
	// //MsgSetRcvFromIP(pMsg, pLocalHostIP);
	// MsgSetHOSTNAME(pMsg, glbl.GetLocalHostName(), ustrlen(glbl.GetLocalHostName()));
	// MsgSetTAG(pMsg, pszTag, ustrlen(pszTag));
	// if (inst) {
	// 	MsgSetRuleset(pMsg, inst->pBindRuleset);
	// }
	// //msgSetPRI(pMsg, pri);
	// // use this later, since we haven't implemented a ratelimitter
	// CHKiRet(submitMsg2(pMsg));
	// //STATSCOUNTER_INC(statsCounter.ctrSubmitted, statsCounter.mutCtrSubmitted);
finalize_it:
	if (iRet != RS_RET_OK) {
		// unexpected for now.
		//STATSCOUNTER_INC(statsCounter.ctrDiscarded, statsCounter.mutCtrDiscarded);
		printf("enqMsg error: %d\n", iRet);
		assert(0);
	}
	RETiRet;
}

extern void goRunInput();

// rsyslog interface
BEGINnewInpInst
CODESTARTnewInpInst
ENDnewInpInst

BEGINbeginCnfLoad
CODESTARTbeginCnfLoad
	assert(pModConf);
dbgprintf("imsimple: begincnfload\n");
DBGPRINTF("imsimple: begincnfload\n");
	loadModConf = pModConf;
	pModConf->pConf = pConf;
	goRunInput();
ENDbeginCnfLoad

BEGINsetModCnf
CODESTARTsetModCnf
ENDsetModCnf


BEGINendCnfLoad
CODESTARTendCnfLoad
ENDendCnfLoad

BEGINcheckCnf
CODESTARTcheckCnf
ENDcheckCnf

BEGINactivateCnf
CODESTARTactivateCnf
ENDactivateCnf

BEGINfreeCnf
CODESTARTfreeCnf
ENDfreeCnf

extern int goCallbackHandler(int, int);

static rsRetVal runInput(thrdInfo_t __attribute__((unused)) *pThrd)
{
	DEFiRet;
	goCallbackHandler(3, 4);
	goRunInput();
	sleep(10);
	RETiRet;
}

BEGINwillRun
CODESTARTwillRun
ENDwillRun

BEGINafterRun
CODESTARTafterRun
ENDafterRun


BEGINisCompatibleWithFeature
CODESTARTisCompatibleWithFeature
ENDisCompatibleWithFeature

BEGINmodExit
CODESTARTmodExit
ENDmodExit


BEGINqueryEtryPt
CODESTARTqueryEtryPt
CODEqueryEtryPt_STD_IMOD_QUERIES
CODEqueryEtryPt_STD_CONF2_QUERIES
CODEqueryEtryPt_STD_CONF2_setModCnf_QUERIES
CODEqueryEtryPt_STD_CONF2_IMOD_QUERIES
CODEqueryEtryPt_IsCompatibleWithFeature_IF_OMOD_QUERIES
	if(iRet == RS_RET_OK)\
		if(*pEtryPoint == NULL) {
			dbgprintf("imsimple: entry point '%s' not present in module\n", name);
			iRet = RS_RET_MODULE_ENTRY_POINT_NOT_FOUND;
		}
	RETiRet;
}


static rsRetVal s_modInit(int iIFVersRequested,
													int *ipIFVersProvided,
													rsRetVal (**pQueryEtryPt)(),
													rsRetVal (*pHostQueryEtryPt)(uchar*, rsRetVal (**)()),
													modInfo_t *pModInfo) {
	DEFiRet;
	rsRetVal (*pObjGetObjInterface)(obj_if_t *pIf);
	assert(pHostQueryEtryPt != NULL);
	iRet = pHostQueryEtryPt((uchar*)"objGetObjInterface", &pObjGetObjInterface);
	if((iRet != RS_RET_OK) || (pQueryEtryPt == NULL) || (ipIFVersProvided == NULL) ||
		(pObjGetObjInterface == NULL)) {
		return (iRet == RS_RET_OK) ? RS_RET_PARAM_ERROR : iRet;
	}
	// now get the obj interface so that we can access other objects
	CHKiRet(pObjGetObjInterface(&obj));

	// template-module end CODESTARTmodInit
	*ipIFVersProvided = CURR_MOD_IF_VERSION;
CODEmodInit_QueryRegCFSLineHdlr
	CHKiRet(objUse(ruleset, CORE_COMPONENT));
	CHKiRet(objUse(glbl, CORE_COMPONENT));
	CHKiRet(objUse(prop, CORE_COMPONENT));
	CHKiRet(objUse(statsobj, CORE_COMPONENT));

	dbgprintf("imsimple: s_modInit()\n");
finalize_it:
	*pQueryEtryPt = queryEtryPt;
	RETiRet;
}

// typedefs
typedef rsRetVal (**ppQueryEtryPt)();
typedef rsRetVal (*pHostQueryEtryPt)(uchar*, rsRetVal (**)());
*/
import "C"

import "unsafe"

import (
	"fmt"
	"sort"
)

//export atest
func atest() C.int {
	//cs := C.CString("Hello from stdio")
	//C.myprint(cs)
	//C.free(unsafe.Pointer(cs))
	return 0
}

//export printTest
func printTest() {
	fmt.Println("hello from golang")
}

//export Sort
func Sort(vals []int) { sort.Ints(vals) }

//export modInit
func modInit(iIFVersRequested C.int,
	ipIFVersProvided *C.int,
	pQueryEtryPt C.ppQueryEtryPt,
	pHostQueryEtryPt C.pHostQueryEtryPt,
	pModInfo *C.modInfo_t) C.rsRetVal {

	return C.s_modInit(iIFVersRequested, ipIFVersProvided, pQueryEtryPt,
		pHostQueryEtryPt, pModInfo)
}

//export goCallbackHandler
func goCallbackHandler(a, b C.int) C.int {
	return a + b
}

//export goRunInput
func goRunInput() {
	// allocate a simple c string
	for i := 1; i < 5; i++ {
		fmt.Println("hello from goRunInput")
		cs := C.CString("Hello from stdio")
		C.enqMsg(nil, cs, C.strlen(cs))
		C.free(unsafe.Pointer(cs))
	}
}

func main() {}

/// ignore these for now.
/*
//export modInit
func modInit(iIFVersRequested C.int,
	ipIFVersProvided *C.int,
	pQueryEtryPt *unsafe.Pointer,
	pHostQueryEtryPt unsafe.Pointer,
	pModInfo *C.modInfo_t) C.rsRetVal {
	return C.s_modInit(iIFVersRequested, ipIFVersProvided, pQueryEtryPt,
		pHostQueryEtryPt, pModInfo)

}
*/

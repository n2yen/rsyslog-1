#! /usr/bin/env python
# -*- coding: utf-8 -*-

# 	* Copyright (C) 2013 Adiscon GmbH.
#	* This file is part of RSyslog
#	* 
#	* This script processes impstats logfiles and splits them into csv files
#	*

import sys
import datetime
import time
import re
import os

# Set default variables
szInput = "rsyslog-stats.log"
szOutputDir = "./"
bSingleObjectOutput = True
bHelpOutput = False
bEnableCharts = False
bLogarithmicChart = False
bFilledLineChart = False
szChartsFormat = "svg"

# Helper variables
nLogLineNum = 0
nLogFileCount = 0
szChartOptionalArgs = ""

# Create regex for logline
loglineregex = re.compile(r"(...)(?:.|..)([0-9]{1,2}) ([0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}) ([a-zA-Z0-9_\-\.]{1,256}) ([A-Za-z0-9_\-\/\.]{1,32}): (.*?): (.*?) \n")
# Array Indexes
LN_MONTH = 1;LN_DAY = 2;LN_TIME = 3;LN_HOST = 4;LN_SYSLOGTAG = 5;LN_LOGOBJECT = 6;LN_LOGDATA = 7

# Init result with file handles
outputFiles = {}

# Open Errorlog on init
errorlog = open("statslog-splitter.corrupted.log", 'w')

# Process Arguments
for arg in sys.argv[-4:]:
	if arg.find("--input=") != -1:
		szInput = arg[8:]
	elif arg.find("--outputdir=") != -1:
		szOutputDir = arg[12:]
	elif arg.find("--singlefile") != -1:
		bSingleObjectOutput = True
	elif arg.find("--enablecharts") != -1:
		bEnableCharts = True
	elif arg.find("--chartsformat=") != -1:
		szChartsFormat = arg[15:]
	elif arg.find("--logarithmic") != -1:
		bLogarithmicChart = True
	elif arg.find("--filledlinechart") != -1:
		bFilledLineChart = True
	elif arg.find("--h") != -1 or arg.find("-h") != -1 or arg.find("--help") != -1:
		bHelpOutput = True

if bHelpOutput: 
	print "\n\nStatslog-splitter command line options:"
	print "======================================="
	print "	--input=<filename>		Contains the path and filename of your impstats logfile. "
	print "					Default is 'rsyslog-stats.log' \n"
	print "	--outputdir=<dir>		Output directory to be used. "
	print "					Default is current directory. "
	print "	--h / -h / --help		Displays this help message. \n"
	print "	--singlefile			Splits the stats logfile into single CSV Files"
	print "					Default is enabled."
	print " --enablecharts			Generate Charts for each exported CSV File."
	print "					Default is disabled."
	print " --chartsformat=<svg|png>	Format which should be used for Charts."
	print "					Default is svg format"
	print " --logarithmic			Uses Logarithmic to scale the Y Axis, maybe useful in some cases. Default is OFF"
	print " --filledlinechart		Use filled lines on Linechart, maybe useful in some cases. Default is OFF"
	print "\n	Sampleline: ./statslog-splitter.py singlefile --input=rsyslog-stats.log --outputdir=/home/user/csvlogs/ --enablecharts --chartsformat=png"
elif bSingleObjectOutput:
	inputfile = open(szInput, 'r')
	for line in inputfile.readlines():
		if line.find("rsyslogd-pstats") != -1:
			# Init variables
			aFields = []
			aData = []

			# Parse IMPStats Line!
			result = loglineregex.split(line)
			# Found valid logline, save into file! 
			if len(result) >= LN_LOGDATA and result[LN_SYSLOGTAG] == "rsyslogd-pstats":
				# Convert Datetime!
				filedate = datetime.datetime.strptime(result[LN_MONTH] + " " + str(datetime.datetime.now().year) + " " + result[LN_DAY] + " " + result[LN_TIME] ,"%b %Y %d %H:%M:%S")

				# Split logdata into Array
				aProperties = result[LN_LOGDATA].split(" ")
				for szProperty in aProperties:
					aProperty = szProperty.split("=")
					aFields.append(aProperty[0])	# Append FieldID
					if len(aProperty) > 1:
						aData.append(aProperty[1])	# Append FieldData
					else: 
						errorlog.write("Corrupted Logline at line " + str(nLogLineNum) + " failed to parse: " + line)
						break

				# Remove invalid characters for filename!
				szFileName = re.sub("[^a-zA-Z0-9]", "_", result[LN_LOGOBJECT]) + ".csv"

				# Open file for writing!
				if szFileName not in outputFiles:
       					print "Creating file : " + szOutputDir + "/" + szFileName
					outputFiles[szFileName] = open(szOutputDir + "/" + szFileName, 'w')
					nLogFileCount += 1
					
					# Output CSV Header
					outputFiles[szFileName].write("Date;")
					outputFiles[szFileName].write("Host;")
					outputFiles[szFileName].write("Object;")
					for szField in aFields:
						outputFiles[szFileName].write(szField + ";")
					outputFiles[szFileName].write("\n")
				#else:
				#	print "already open: " + szFileName
				
				# Output CSV Data
				outputFiles[szFileName].write(filedate.strftime("%Y/%b/%d %H:%M:%S") + ";")
				outputFiles[szFileName].write(result[LN_HOST] + ";")
				outputFiles[szFileName].write(result[LN_LOGOBJECT] + ";")
				for szData in aData:
					outputFiles[szFileName].write(szData + ";")
				outputFiles[szFileName].write("\n")

				#print result[LN_LOGOBJECT]
				#print result[LN_LOGDATA]
			else:
				print "Fail parsing logline: "
				print result
				break


			# Increment helper counter
			nLogLineNum += 1 

			#print result
			#break

			#print "IMPStats Line found: " + line

	# Close outfiles
	for outFileName in outputFiles:
		outputFiles[outFileName].close()

	# Close input file
	inputfile.close()

	print "\n	File " + szInput + " has been processed"
	print "	" + str(nLogFileCount) + " Logfiles have been exported to " + szOutputDir
	
	if bEnableCharts: 
		# Open HTML Code
		szHtmlCode =	"<!DOCTYPE html><html><head></head><body><center>"

		# Add optional args 
		if bLogarithmicChart: 
			szChartOptionalArgs += " --logarithmic"
		if bFilledLineChart: 
			szChartOptionalArgs += " --filledlinechart"

		# Default SVG Format!
		if szChartsFormat.find("svg") != -1:
			for outFileName in outputFiles:
				iReturn = os.system("./statslog-graph.py " + szChartOptionalArgs + " --input=" + szOutputDir + "/" + outFileName + "")
				print "Chart SVG generated for '" + outFileName + "': " + str(iReturn)
				szHtmlCode +=	"<figure><embed type=\"image/svg+xml\" src=\"" + outFileName[:-4] + ".svg" + "\" />" + "</figure><br/><br/>"
		# Otherwise PNG Output!
		else: 
			for outFileName in outputFiles:
				iReturn = os.system("./statslog-graph.py " + szChartOptionalArgs + " --input=" + szOutputDir + "/" + outFileName + " --convertpng")
				print "Chart PNG generated for '" + outFileName + "': " + str(iReturn)
				szHtmlCode +=	"<img src=\"" + outFileName[:-4] + ".png" + "\" width=\"800\" height=\"600\"/>" + "<br/><br/>"

		print "	" + str(nLogFileCount) + " Charts have been written " + szOutputDir
		
		# Close HTML Code
		szHtmlCode +=	"</center></body></html>"

		# Write HTML Index site!
		outHtmlFile = open(szOutputDir + "/index.html", 'w')
		outHtmlFile.write(szHtmlCode)
		outHtmlFile.close()
		print "	HTML Index with all charts has been written to " + szOutputDir + "/index.html"

	print "\n\n"

# Close Error log on exit
errorlog.close()

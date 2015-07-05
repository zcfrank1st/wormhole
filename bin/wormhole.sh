#!/bin/sh

source /etc/profile
export LD_LIBRARY_PATH=/usr/local/hadoop/hadoop-release/lib/native/Linux-amd64-64:/usr/local/hadoop/lzo/lib
export LANG="en_US.UTF-8"
export LION_PROJECT="mercury"

CURR_DIR=`pwd`
cd `dirname "$0"`/..
WORMHOLE_HOME=`pwd`

#set JAVA_OPTS
JAVA_OPTS=" -Xms1g -Xmx4g -Xmn256m -Xss2048k -XX:PermSize=128m -XX:MaxPermSize=512m"
#gain java bin
JAVA_BIN=`which java`

#performance Options
#JAVA_OPTS="$JAVA_OPTS -XX:+AggressiveOpts"
#JAVA_OPTS="$JAVA_OPTS -XX:+UseBiasedLocking"
#JAVA_OPTS="$JAVA_OPTS -XX:+UseFastAccessorMethods"
#JAVA_OPTS="$JAVA_OPTS -XX:+DisableExplicitGC"
#JAVA_OPTS="$JAVA_OPTS -XX:+UseParNewGC"
#JAVA_OPTS="$JAVA_OPTS -XX:+UseConcMarkSweepGC"
#JAVA_OPTS="$JAVA_OPTS -XX:+CMSParallelRemarkEnabled"
#JAVA_OPTS="$JAVA_OPTS -XX:+UseCMSCompactAtFullCollection"
#JAVA_OPTS="$JAVA_OPTS -XX:+UseCMSInitiatingOccupancyOnly"
#JAVA_OPTS="$JAVA_OPTS -XX:CMSInitiatingOccupancyFraction=75"
#JAVA_OPTS="$JAVA_OPTS -XX:LargePageSizeInBytes=128m"

#log print Options
#JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCApplicationStoppedTime"
#JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCTimeStamps"
#JAVA_OPTS="$JAVA_OPTS -XX:+PrintGCDetails"
#==========================================================================

#start
RUN_CMD="$JAVA_BIN -classpath \"${WORMHOLE_HOME}/lib/*:${WORMHOLE_HOME}/conf/\""
RUN_CMD="$RUN_CMD $JAVA_OPTS"
RUN_CMD="$RUN_CMD com.dp.nebula.wormhole.engine.core.Engine ${WORMHOLE_HOME}/tasks/$@"

echo $RUN_CMD
eval $RUN_CMD
#==========================================================================
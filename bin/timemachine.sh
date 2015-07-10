#!/usr/bin/env bash

CURR_DIR=`pwd`
cd `dirname "$0"`/..
WORMHOLE_HOME=`pwd`
JAVA_BIN=`which java`

RUN_CMD="$JAVA_BIN -classpath \"${WORMHOLE_HOME}/lib/*:${WORMHOLE_HOME}/conf/\""

if [ -z "$2" ];then
    RUN_CMD="$RUN_CMD com.dp.nebula.wormhole.tools.TimeMachine $1"
else
    RUN_CMD="$RUN_CMD com.dp.nebula.wormhole.tools.TimeMachine $1 $2"
fi
echo $RUN_CMD
eval $RUN_CMD

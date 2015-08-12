#!/usr/bin/env bash
# usage $1 absolute path

CURR_DIR=`pwd`
cd `dirname "$0"`/..
WORMHOLE_HOME=`pwd`

if [ -z "$1" ]
then
    echo "usage: sh runtask.sh [path : file absolute path]"
else
    CURRENT_FILE=$1_`date "+%Y%m%d"`
    cp $1 $CURRENT_FILE
    TIMEMACHINE="sh ${WORMHOLE_HOME}/bin/timemachine.sh $CURRENT_FILE $2"
    echo $TIMEMACHINE
    echo "starting to run timemachine..."
    eval $TIMEMACHINE
    if [ "$?" -ne 0 ];then
        echo "timemachine exec --- failed!"
        exit
    fi
    WORMHOLE="sh ${WORMHOLE_HOME}/bin/wormhole.sh $CURRENT_FILE"
    echo $WORMHOLE
    echo "starting to run wormhole..."
    eval $WORMHOLE
    if [ "$?" -ne 0 ];then
        echo "wormhole exec --- failed!"
        exit
    fi
fi
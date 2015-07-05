#!/usr/bin/env bash
# usage $1 absolute path

CURR_DIR=`pwd`
cd `dirname "$0"`/..
WORMHOLE_HOME=`pwd`

if [ -z "$1" ]
then
    echo "usage: sh runtask.sh [path : file absolute path]"
else
    TIMEMACHINE="sh ${WORMHOLE_HOME}/bin/timemachine.sh $1"
    echo $TIMEMACHINE
    eval $TIMEMACHINE
    echo "starting to run timemachine..."
    if [ "$?" -ne 0 ];then
        echo "timemachine exec --- failed!"
        exit
    fi
    WORMHOLE="sh ${WORMHOLE_HOME}/bin/wormhole.sh $1"
    echo $WORMHOLE
    eval $WORMHOLE
    echo "starting to run wormhole..."
    if [ "$?" -ne 0 ];then
        echo "wormhole exec --- failed!"
        exit
    fi
fi
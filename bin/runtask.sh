#!/usr/bin/env bash
# usage $1 absolute path

CURR_DIR=`pwd`
cd `dirname "$0"`/..
WORMHOLE_HOME=`pwd`

CMD="sh ${WORMHOLE_HOME}/bin/timemachine.sh $1"
CMD="$CMD;sh ${WORMHOLE_HOME}/bin/wormhole.sh $1"
echo $CMD
eval $CMD
#!/usr/bin/env bash
# usage $1 absolute path

CMD="sh ./timemachine.sh $1"
CMD="$CMD;sh ./wormhole.sh $1"

echo $CMD
eval $CMD
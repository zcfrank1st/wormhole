#!/usr/bin/env bash

## 1. 拉取远程包
## 2. 解压包
## 3. rsync

WORMHOLE_ROOT="/data/deploy/chao.zhang/wormhole-zebra/"
JAR_NAME="nebula-wormhole-product-0.2.0-SNAPSHOT.jar"
FTP_DIR="http://10.1.1.81/product-datadp-wormhole/"

if [ "$1" != "" ];then
    rm -rf $WORMHOLE_ROOT
    mkdir -p $WORMHOLE_ROOT
    cd $WORMHOLE_ROOT
    rm -rf *
    wget -O "./$JAR_NAME" "$FTP_DIR$1/$JAR_NAME"
    jar -xvf $JAR_NAME

    src=$WORMHOLE_ROOT
    tgt=$WORMHOLE_ROOT
    OUTPUT_LOG=/tmp/output-$$.log
    ERROR_LOG=/tmp/error-$$.log
    MACHINES=("10.1.6.49" "10.2.2.22" "10.2.2.23" "10.1.110.49" "10.1.115.31" "10.1.115.32" "10.2.2.86" "10.2.2.87" "10.1.110.94" "10.1.110.96")

    for machine in ${MACHINES[@]}
    do
        echo -e "rsync --delete -av -e \"ssh -p58422\" $src deploy@$machine:$tgt" | sh
    done
else
    echo "usage: deploy [build_timestamp]  ----- 'build_timestamp' is from jenkins "
fi
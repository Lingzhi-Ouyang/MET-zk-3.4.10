#!/bin/bash

## kill current running zookeeper processes
ps -ef | grep zookeeper | grep -v grep | awk '{print $2}' | xargs kill -9

SCRIPT_DIR=$(cd $(dirname "$0") || exit;pwd)
WORKING_DIR=$(cd "$SCRIPT_DIR"/../.. || exit;pwd)

echo "$WORKING_DIR"

cd "$WORKING_DIR"/zk-test/test || exit

tag=$(date "+%y-%m-%d-%H-%M-%S")
mkdir $tag
cp zk_log.properties $tag
nohup java -jar ../zookeeper-ensemble/target/zookeeper-ensemble-jar-with-dependencies.jar zookeeper.properties $tag > $tag/$tag.out 2>&1 &
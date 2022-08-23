#!/bin/bash

## kill current running zookeeper processes
ps -ef | grep zookeeper | grep -v grep | awk '{print $2}' | xargs kill -9

SCRIPT_DIR=$(cd $(dirname $0);pwd)
WORKING_DIR=$(cd $SCRIPT_DIR/../..;pwd)

echo $WORKING_DIR

## build ZooKeeper
#cd $WORKING_DIR/zookeeper-3.4.13 && ant

# build HitMC
cd $WORKING_DIR/zk-test && mvn clean install -DskipTests
cd $WORKING_DIR/zk-test/test

tag=`date "+%y-%m-%d-%H-%M-%S"`
mkdir $tag
cp zk_log.properties $tag
# enable assertions!
nohup java -ea -jar ../zookeeper-ensemble/target/zookeeper-ensemble-jar-with-dependencies.jar zookeeper.properties $tag > $tag/$tag.out 2>&1 &
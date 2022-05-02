#!/bin/bash

## kill current running zookeeper processes
ps -ef | grep zookeeper | grep -v grep | awk '{print $2}' | xargs kill -9
#rm -fr 1

# run within ./test
#nohup java -jar ../zookeeper-ensemble/target/zookeeper-ensemble-jar-with-dependencies.jar zookeeper.properties > test.out 2>&1 &

tag=`date "+%y-%m-%d-%H-%M-%S"`
mkdir $tag
cp zk_log.properties $tag
nohup java -jar ../zookeeper-ensemble/target/zookeeper-ensemble-jar-with-dependencies.jar zookeeper.properties $tag > $tag/$tag.out 2>&1 &

#java -jar ../zookeeper-ensemble/target/zookeeper-ensemble-jar-with-dependencies.jar zookeeper.properties  >/dev/null 2>&1 &
#java -jar ../zookeeper-ensemble/target/zookeeper-ensemble-jar-with-dependencies.jar zookeeper.properties
#java -classpath ../zookeeper-ensemble/target/classes:\
#                ../api/target/classes:\
#                ../server/target/classes:\
#                /local/m2/repository/org/slf4j/slf4j-api/1.7.19/slf4j-api-1.7.19.jar:\
#                /local/m2/repository/org/slf4j/jcl-over-slf4j/1.7.19/jcl-over-slf4j-1.7.19.jar:\
#                /local/m2/repository/ch/qos/logback/logback-classic/1.1.6/logback-classic-1.1.6.jar:\
#                /local/m2/repository/ch/qos/logback/logback-core/1.1.6/logback-core-1.1.6.jar:\
#                /local/m2/repository/org/springframework/spring-context/4.2.5.RELEASE/spring-context-4.2.5.RELEASE.jar:\
#                /local/m2/repository/org/springframework/spring-aop/4.2.5.RELEASE/spring-aop-4.2.5.RELEASE.jar:\
#                /local/m2/repository/aopalliance/aopalliance/1.0/aopalliance-1.0.jar:\
#                /local/m2/repository/org/springframework/spring-beans/4.2.5.RELEASE/spring-beans-4.2.5.RELEASE.jar:\
#                /local/m2/repository/org/springframework/spring-core/4.2.5.RELEASE/spring-core-4.2.5.RELEASE.jar:\
#                /local/m2/repository/org/springframework/spring-expression/4.2.5.RELEASE/spring-expression-4.2.5.RELEASE.jar \
#                org.mpisws.hitmc.zookeeper.ZookeeperMain zookeeper.properties >/dev/null 2>&1 &

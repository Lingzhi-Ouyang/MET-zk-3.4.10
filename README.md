# HitMC

This project builds a model checker for ZooKeeper ensembles using the idea of hitting families of schedules 
(Chistikov, Majumdar, and Niksic, CAV 2016). More details can be found in the paper [Trace Aware Random Testing for Distributed Systems](https://dl.acm.org/doi/pdf/10.1145/3360606). 

The project is develeped based on the implementation [here](https://gitlab.mpi-sws.org/rupak/hitmc).



## Build Instructions

Prerequisites are [Apache Ant](http://ant.apache.org/) and [Apache Maven](http://maven.apache.org/) (at least version 3.x).



The test can be built and run using the script below:

```bash
./HitMC/test/buildAndTest.sh
```



Or you can configure, build and run the test step by step.

First build ZooKeeper:

1. Enter zookeeper-3.4.13
2. Execute `ant`

Then build HitMC:

1. Enter HitMC
2. Execute `mvn install`
3. Enter HitMC/zookeeper-wrapper
4. Execute `mvn package`
5. Enter HitMC/zookeeper-ensemble
6. Execute `mvn package`

Then start the test:

1. Enter test
2. configure your test parameters in `zookeeper.properties`
3. Execute `./test.sh`

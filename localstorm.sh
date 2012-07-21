#!/bin/sh

#export MAVEN_OPTS="-server -XX:+UseParallelGC -XX:+UseParallelOldGC -XX:+PrintGC -XX:+PrintGCDetails -Xms128m -Xmx128m -XX:NewRatio=3"
export MAVEN_OPTS="-server -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+PrintGC -XX:+PrintGCDetails -Xms128m -Xmx128m -XX:NewRatio=3 -XX:+CMSIncrementalMode -XX:+CMSIncrementalPacing"
#export MAVEN_OPTS="-server -XX:+UseG1GC -Xms128m -Xmx128m"
mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=weathered.LoadISDStorm -Dlog4j.configuration=log4j.xml -Dexec.args="http://127.0.0.1:8080/010010-99999-2012 127.0.0.1 1 4 2"

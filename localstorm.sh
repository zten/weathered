#!/bin/sh

#export MAVEN_OPTS="-server -XX:+UseParallelGC -XX:+UseParallelOldGC -XX:+PrintGC -XX:+PrintGCDetails -Xms128m -Xmx128m -XX:NewRatio=3"
export MAVEN_OPTS="-server -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+PrintGC -XX:+PrintGCDetails -Xms128m -Xmx128m -XX:NewRatio=3 -XX:+CMSIncrementalMode -XX:+CMSIncrementalPacing"
#export MAVEN_OPTS="-server -XX:+UseG1GC -Xms128m -Xmx128m"
mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=weathered.LoadISDStorm -Dweathered.dataDir=/Users/dubba/Documents/workspace/weathered/testdata -Dlog4j.configuration=log4j.xml

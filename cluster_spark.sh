#!/bin/sh

export JAVA_HOME=/usr/java/jdk1.7.0_67-cloudera
export SCALA_HOME=/usr
export CDH_HOME=/opt/cloudera/parcels/CDH/
export CLASSPATH=".:$CDH_HOME/jars/*"
SPARK_HOME=/usr

task=Task${1}

echo --- Deleting
rm ${task}.jar
rm ${task}*.class

echo --- Compiling
$SCALA_HOME/bin/scalac -J-Xmx1g ${task}.scala
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf ${task}.jar ${task}*.class

echo --- Running
INPUT=${2}
OUTPUT=/user/${USER}/output_spark_${task}/

hdfs dfs -rm -R $OUTPUT
time $SPARK_HOME/bin/spark-submit --class ${task} ${task}.jar $INPUT $OUTPUT

hdfs dfs -ls $OUTPUT

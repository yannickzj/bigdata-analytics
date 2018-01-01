#!/bin/sh

export JAVA_HOME=/usr/java/jdk1.7.0_67-cloudera
export HADOOP_HOME=/usr
export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`

task=Task${1}

echo --- Deleting
rm ${task}.jar
rm ${task}*.class

echo --- Compiling
$JAVA_HOME/bin/javac ${task}.java
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf ${task}.jar ${task}*.class

echo --- Running
INPUT=${2}
OUTPUT=/user/${USER}/output_hadoop_${task}/
OUTPUT_TMP=/user/${USER}/output_hadoop_${task}_tmp

hdfs dfs -rm -R $OUTPUT
hdfs dfs -rm -R $OUTPUT_TMP
time $HADOOP_HOME/bin/hadoop jar ${task}.jar ${task} $INPUT $OUTPUT

hdfs dfs -ls $OUTPUT

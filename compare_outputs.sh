#!/bin/bash

OUTPUT_SPARK=output_spark_Task${1}
OUTPUT_HADOOP=output_hadoop_Task${1}
OUTPUT_TMP=tmp/

hdfs dfs -cat $OUTPUT_SPARK/* | sort > ${OUTPUT_TMP}normalized_spark.txt
hdfs dfs -cat $OUTPUT_HADOOP/* | sort > ${OUTPUT_TMP}normalized_hadoop.txt

echo Diffing Spark and Hadoop outputs:
diff ${OUTPUT_TMP}normalized_spark.txt ${OUTPUT_TMP}normalized_hadoop.txt

if [ $? -eq 0 ]; then
    echo Outputs match.
else
    echo Outputs do not match. Looks for bugs.
fi

rm ${OUTPUT_TMP}normalized_spark.txt
rm ${OUTPUT_TMP}normalized_hadoop.txt

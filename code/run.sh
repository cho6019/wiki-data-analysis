#!/bin/bash


SPARK_HOME=/home/sgcho0907/app/spark-3.5.1-bin-hadoop3
DS=$1
PY_PATH=$2

echo "DS ===> $DS"
#/home/sgcho0907/app/spark-3.5.1-bin-hadoop3/bin/spark-submit \
$SPARK_HOME/bin/spark-submit \
--master spark://spark-nunininu.asia-northeast3-c.c.neat-gasket-455501-e5.internal:7077 \
--executor-memory 2G \
--executor-cores 2 \
/home/sgcho0907/code/to_parquet.py $DS

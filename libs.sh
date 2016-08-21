#!/usr/bin/env bash

LIBS_DIR=libs
# Spark variables
SPARK_VERSION=1.6.1
SPARK_PACKAGE=spark-${SPARK_VERSION}-bin-hadoop2.6

SPARK_URL=http://ftp.cixug.es/apache/spark/spark-${SPARK_VERSION}/spark-{SPARK_VERSION}-bin-hadoop2.6.tgz

cd ${LIBS_DIR}
curl -s ${SPARK_URL} > ${SPARK_PACKAGE}.tgz
tar xzf ${SPARK_PACKAGE}
cp ${SPARK_PACKAGE}/lib/spark-assembly-${SPARK_VERSION}-hadoop2.6.0.jar ./
rm -Rf spark-${SPARK_VERSION}-bin-hadoop2.6 && rm ${SPARK_PACKAGE}
#!/bin/bash

if [ $# -eq 0 ]; then
  echo "No argument."
  echo "Usage: ./csle-submit.sh class_name"
  exit 255
fi

RUN_CLASS_NAME=$1

JAVA=java
if [ "x$JAVA_HOME" != "x" ]; then
  JAVA=$JAVA_HOME/bin/java
fi

CSLE_LIB_HOME=$HOME/csleClientLib

for f in ${CSLE_LIB_HOME}/*.jar ; do
  CLASSPATH=$CLASSPATH:$f
done

$JAVA -cp $CLASSPATH $RUN_CLASS_NAME


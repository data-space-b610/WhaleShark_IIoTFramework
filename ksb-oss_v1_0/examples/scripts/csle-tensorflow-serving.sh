#!/bin/bash

if [ "x$KSB_HOME" == "x" ]; then
  export KSB_HOME=$PWD
fi

CSLE_LIB_DIR=$KSB_HOME/jars
if [ ! -d "$CSLE_LIB_DIR" ]; then
  CSLE_LIB_DIR=$KSB_HOME/jars
fi

CSLE_CLIENT_LIB_DIR=$PWD/csleClientLib
if [ ! -d "$CSLE_CLIENT_LIB_DIR" ]; then
  CSLE_CLIENT_LIB_DIR=$HOME/csleClientLib
fi

if [ "x$JAVA_HOME" != "x" ]; then
  JAVA=$JAVA_HOME/bin/java
else
  if [ -d "$PWD/jdk" ]; then
    JAVA=$PWD/jdk/bin/java
  else
    JAVA=java
  fi
fi

if [ -d "$CSLE_LIB_DIR" ]; then
  for f in $CSLE_LIB_DIR/*.jar ; do
    CLASSPATH=$CLASSPATH:$f
  done
else
  for f in $CSLE_CLIENT_LIB_DIR/*.jar ; do
    CLASSPATH=$CLASSPATH:$f
  done
fi

RUN_CLASS_NAME="ksb.csle.tools.utils.TensorflowServingUtil"

echo "-----------------------------------------------------------------------"
echo " KSB_HOME: $KSB_HOME"
echo " CSLE_LIB: $CSLE_LIB_DIR"
echo " CSLE_CLIENT_LIB: $CSLE_CLIENT_LIB_DIR"
echo " JAVA: $JAVA"
echo "-----------------------------------------------------------------------"
$JAVA -cp $CLASSPATH $RUN_CLASS_NAME $@


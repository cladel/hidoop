#!/bin/bash

HIDOOP_CLASSES=/home/cla/N7/2A/hidoop/out/production/hidoop
folder=$HIDOOP_HOME/data/

cd $HIDOOP_CLASSES

echo "Starting server..."

java hdfs.HdfsServer &  p1=$!
sleep 2

echo ""
echo "Launching tests..."
time java hdfs.HdfsClient -w $@
time java hdfs.HdfsClient -r $1

echo $1
diff ${folder}$1 ${folder}r_$1


kill $p1


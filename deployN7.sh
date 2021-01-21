#!/bin/bash

# deployN7 user machine destfolder


scp -r src $1@$2:$3
ssh $1@$2 " [[ -z  \$HIDOOP_CLASSES ]] && HIDOOP_CLASSES=\$HIDOOP_HOME/src ; cd $3/src && javac -d \$HIDOOP_CLASSES ordo/WorkerImpl.java hdfs/HdfsServer.java"



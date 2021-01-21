#!/bin/bash

# Deployer sur une machine distante

#Verifier parametres
if [ "$#" -ne 3 ]; then
    echo "Use : ./deployN7.sh <sshuser> <machine> <destfolder>"
    exit 1
fi

scp -r src $1@$2:$3
ssh $1@$2 " [[ -z  \$HIDOOP_CLASSES ]] && HIDOOP_CLASSES=\$HIDOOP_HOME/src ; cd $3/src && javac -d \$HIDOOP_CLASSES ordo/WorkerImpl.java hdfs/HdfsServer.java application/MyMapReduce.java"


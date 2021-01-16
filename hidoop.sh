#!/bin/bash


# Trouver les adresses ip des serveurs dans conf.xml (sans dupliqués)
nodes=($(grep -oP '(?<=<node)[^/]+(?=/>)' "${HIDOOP_HOME}conf.xml" | grep -oP '(?<=ip=")[a-zA-Z0-9.]+(?=")' | sort -u))


cd $HIDOOP_CLASSES

function start()
{
classes_dir=$(pwd)

echo "Starting servers..."
# Lancer chaque serveur
for s in ${nodes[@]}
do

echo " - $s"
ssh $s "cd $classes_dir && java hdfs.HdfsServer & java ordo.WorkerImpl &"

done

echo "Hidoop servers ready."
}

# A la fermeture tuer les serveurs
function quit()
{
   for s in ${nodes[@]}
   do

	ssh $s "pkill -f 'java hdfs.HdfsServer' &&  pkill -f 'java ordo.WorkerImpl'"

   done
   exit 0
}

while true
do
	read -p " > " cmd;

	case $cmd in
	exit)
	  quit
	  ;;
	start)
	  start
	  ;;
	hdfs*)
    eval "${cmd/hdfs/'java hdfs.HdfsClient'}"
	  ;;
	MyMapReduce*)
	  eval "${cmd/MyMapReduce/'java application.MyMapReduce'}"
	  ;;
	*)
	  eval $cmd
	  ;;
	esac
done



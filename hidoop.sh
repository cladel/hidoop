#!/bin/bash


# Trouver les adresses ip des serveurs dans conf.xml (sans dupliqués)
nodes=($(grep -oP '(?<=<node)[^/]+(?=/>)' "${HIDOOP_HOME}/config/conf.xml" | grep -oP '(?<=ip=")[a-zA-Z0-9.]+(?=")' | sort -u))

# Valeur par défaut de HIDOOP_CLASSES
[[ -z "${HIDOOP_CLASSES}" ]] && HIDOOP_CLASSES=$HIDOOP_HOME/src
cd $HIDOOP_CLASSES

function start()
{

echo "Starting servers..."
# Lancer chaque serveur
for s in ${nodes[@]}
do

echo " - $s"
ssh $s "cd $HIDOOP_CLASSES && java hdfs.HdfsServer & java ordo.WorkerImpl &"

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

echo
echo '(**********************)'
echo '(******* HIDOOP *******)'
echo '(*******  v1.0  *******)'
echo '(**********************)'
echo

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
	mmr*)
	  eval "${cmd/mmr/'java application.MyMapReduce'}"
	  ;;
	*)
	  eval $cmd
	  ;;
	esac
done



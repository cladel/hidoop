#!/bin/bash

# Verifier que HIDOOP_HOME est défini
if [[ -z "${HIDOOP_HOME}" ]]; then
  echo "Error: HIDOOP_HOME is undefined."
  exit 1
fi
# Verifier la présence du fichier conf.xml
cd $HIDOOP_HOME
if [ ! -f "config/conf.xml" ]; then
    echo "$HIDOOP_HOME/config/conf.xml not found."
    exit 1
fi

# Valeur par défaut de HIDOOP_CLASSES
[[ -z "${HIDOOP_CLASSES}" ]] && HIDOOP_CLASSES=$HIDOOP_HOME/src


# Shell pour l'application
temp=$(cat <<EOF

# Trouver les adresses ip des serveurs dans conf.xml (sans dupliqués)
nodes=($(grep -oP '(?<=<node)[^/]+(?=/>)' "${HIDOOP_HOME}/config/conf.xml" | grep -oP '(?<=ip=")[a-zA-Z0-9.]+(?=")' | sort -u))


# Démarrage des serveurs
function start()
{

echo "Starting servers..."

# Lancer chaque serveur
for s in \${nodes[@]}
do

  echo " - \$s"
  javacmd=\$(echo "<hdfs.HdfsServer><ordo.WorkerImpl>" | sed "s|<\([^<>]*\)>|nohup java -cp $HIDOOP_CLASSES \1 >> $HIDOOP_HOME/\$s.log 2>\&1 \& |g ")
  ssh -v \$s "export HIDOOP_HOME=$HIDOOP_HOME & \${javacmd}"

done

}

# A la fermeture tuer les serveurs
function stop()
{
   echo "Stopping servers..."

   for s in \${nodes[@]}
   do
	    ssh \$s "pkill -f 'java .*(ordo.*|hdfs.*)' "
   done
}

# Alias fonctionnalités
alias hdfs='java hdfs.HdfsClient'
alias mmr='java application.MyMapReduce'


cd $HIDOOP_CLASSES

PS1="hidoop> "

EOF
)

bash --init-file <(echo "$temp")
#!/bin/bash

#Verifier parametres
if [ "$#" -ne 1 ]; then
    echo "Use : ./hidoop.sh <sshuser>"
fi

# Verifier que HIDOOP_HOME est défini
if [[ -z $HIDOOP_HOME ]]; then
  echo "Error: HIDOOP_HOME is undefined."
  exit 1
fi

# Verifier la présence du fichier conf.xml
if [ ! -f "$HIDOOP_HOME/config/conf.xml" ]; then
    echo "$HIDOOP_HOME/config/conf.xml not found."
    exit 1
fi

# Valeur par défaut de HIDOOP_CLASSES
[[ -z  $HIDOOP_CLASSES ]] && HIDOOP_CLASSES=$HIDOOP_HOME/src


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

  # Lancement de hdfsserver et worker sur la machine distante
  javacmd=\$(echo "<hdfs.HdfsServer><ordo.WorkerImpl>" | sed "s|<\([^<>]*\)>|nohup java -cp \\\$HIDOOP_CLASSES \1 >> \\\$HIDOOP_HOME/\$s.log 2>\&1 \& |g ")
  ssh $@@\$s " [[ -z  \\\$HIDOOP_CLASSES ]] && HIDOOP_CLASSES=\\\$HIDOOP_HOME/src ; \${javacmd}"

done

}

# A la fermeture tuer les serveurs
function stop()
{
   echo "Stopping servers..."

   for s in \${nodes[@]}
   do
	    echo " - \$s"
	    ssh $@@\$s "pkill -f 'java .*(ordo.*|hdfs.*)' "
   done
}


function hdfs()
{
   java -cp $HIDOOP_CLASSES hdfs.HdfsClient \$@ 2>>$HIDOOP_HOME/log
}

function mmr()
{
   # Specifier l'ip à utiliser pour java.rmi.server.hostname
   if [[ \$1 = "-ip" ]] ; then
      iprmi=-Djava.rmi.server.hostname=\$2
      cmdargs=\${@: 3}
   else
      cmdargs=\$@
   fi

   java \$iprmi -cp $HIDOOP_CLASSES application.MyMapReduce \$cmdargs 2>>$HIDOOP_HOME/log
}


cd "$HIDOOP_CLASSES" || exit

PS1="hidoop> "

EOF
)

bash --init-file <(echo "$temp")
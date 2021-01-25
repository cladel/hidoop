#!/bin/bash

#Verifier parametres
if [ "$#" -ne 1 ]; then
    echo "Use : $0 <sshuser>"
    exit 1
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
declare -r NODES="$(grep -oP '(?<=<node)[^/]+(?=/>)' "${HIDOOP_HOME}/config/conf.xml" | grep -oP '(?<=ip=")[a-zA-Z0-9.]+(?=")' | sort -u)"

# Constantes utiles
export SSHUSER=$1
export magenta="\001\033[1;95m\002"
export delimiter="\001\033[0;00m\002"
export pnamenode=''
export NODES


# Démarrage des serveurs
function start()
{


echo "Starting namenode..."
read < <( java -cp $HIDOOP_CLASSES config.AppData >> $HIDOOP_HOME/log & echo \$! ) pnamenode
sleep 0.5

echo "Starting servers..."

# Lancer chaque serveur
for s in \$NODES
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

   echo "Stopping namenode..."
   if [[ -z \$pnamenode ]]; then
        kill \$pnamenode
        wait \$pnamenode 2>/dev/null
   else 
        pkill -f 'java .* config.AppData'
   fi

   sleep 0.5

   echo "Stopping servers..."

   for s in \$NODES
   do
	    echo " - \$s"
	    ssh $@@\$s "pkill -f 'java .*(ordo.*|hdfs.*)' "
        
   done
}


function hdfs()
{
   #java hdfs.HdfsClient \$@ 2> >("sed -e \"s/^/\$(date -Iseconds) /\" ">>$HIDOOP_HOME/log)
   java -cp $HIDOOP_CLASSES hdfs.HdfsClient \$@ 2>>$HIDOOP_HOME/log
}

function mmr()
{
   if [[ \$1 = "-ip" ]] ; then
      iprmi=-Djava.rmi.server.hostname=\$2
      cmdargs=\${@: 3}
   else
      cmdargs=\$@
   fi

   java \$iprmi -cp $HIDOOP_CLASSES application.MyMapReduce \$cmdargs 2>>$HIDOOP_HOME/log
}

function printconf()
{
   cat \$HIDOOP_HOME/config/conf.xml
}

function deploy()
{
   source \$HIDOOP_HOME/scripts/hidoop-deploy
}

export -f start
export -f stop
export -f hdfs
export -f mmr
export -f printconf

function monitoring()
{
   source \$HIDOOP_HOME/scripts/hidoop-monitoring \$@
}





cd "$HIDOOP_HOME" || exit

PS1="\${magenta}hidoop>\${delimiter} "


EOF
)

bash --init-file <(echo "$temp")

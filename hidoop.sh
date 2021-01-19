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


# Script d'initialisation du shell pour l'application
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

ssh -v \$s "export HIDOOP_HOME=$HIDOOP_HOME & nohup java -cp $HIDOOP_CLASSES hdfs.HdfsServer >> $HIDOOP_HOME/$s.log 2>&1 & nohup java -cp $HIDOOP_CLASSES ordo.WorkerImpl >> $HIDOOP_HOME/$s.log 2>&1 & "

done

echo "Hidoop servers ready."
}

# A la fermeture tuer les serveurs
function stop()
{
   for s in \${nodes[@]}
   do

	ssh -v \$s "pkill -f 'java .*(ordo.*|hdfs.*)' "

   done
   exit 0
}

# Alias fonctionnalités
alias hdfs='java hdfs.HdfsClient'
alias mmp='java application.MyMapReduce'


cd $HIDOOP_CLASSES

echo
echo '(**********************)'
echo '(******* HIDOOP *******)'
echo '(*******  v1.0  *******)'
echo '(**********************)'
echo

PS1="hidoop> "

EOF
)

# Lancement du shell
bash --init-file <(echo "$temp")
Projet **HIDOOP**
-------------------------------

Ce répertoire correspond à l'arborescence de fichiers suivante :

- le répertoire **config** contient les fichiers d'initialisation pouvant être utiles lors du lancement de la plateforme
- le répertoire **data** accueille les fichiers de données de l'application
- le répertoire **doc** accueille les rapports attendus
- le répertoire **src** contient les codes sources. Ce répertoire contient lui-même les sous-répertoires suivants
  - application, pour le code des applications
  - config, pour les utilitaires de configuration
  - formats, pour la spécification et la réalisation des formats
  - hdfs, pour la mise en œuvre de hdfs
  - ordo pour l'ordonnancement et le contrôle des tâches Map/Reduce

##### Configuration de Hidoop
La variable système **HIDOOP_HOME** est nécessaire au fonctionnement de Hidoop. Il s'agit de la localisation du répertoire *hidoop*, utilisé dans config.Projet.PATH .

  <span style="color:green">*Commande : **export HIDOOP_HOME=path/to/hidoop***</span>

La configuration se fait via un fichier *conf.xml* placé dans le répertoire **HIDOOP_HOME/config/**.
Un fichier d'exemple est donné ci-dessous :

    <?xml version="1.0" encoding="UTF-8"?>
    <config metadata="meta">
        <default-chunk-size value="64" unit="bytes" />
        <servers>
        <node ip="127.0.0.1"/>
        <node ip="chewie"/>
        <node ip="yoda"/>
        </servers>
    </config>

*Note : les unités de tailles supportées sont bytes, kB, MB, GB.*

Ce fichier est utilisé par la classe ***config.AppData***. 

##### Script de lancement 
Le script *hidoop.sh* permet de lancer / arrêter automatiquement les serveurs indiqués dans le fichier de configuration *conf.xml* via ssh (machines N7).
Le répertoire où les commandes sont exécutées est **HIDOOP_HOME/src** par défaut, mais il est possible de définir une variable système **HIDOOP_CLASSES** qui indique la localisation des classes java de l'application.

On peut lancer n'importe quelle commande, plus les suivantes :  
 - ***start*** pour lancer les serveurs 
 - ***exit*** pour quitter et fermer les serveurs
 - ***hdfs*** raccourci pour java hdfs.HdfsClient, suivi des mêmes arguments
 - ***mmr*** raccourci pour java application.MyMapReduce, suivi des mêmes arguments 



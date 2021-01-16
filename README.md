Projet **HIDOOP**
-------------------------------

Ce répertoire correspond à l'arborescence de fichiers que doivent **impérativement**
respecter vos rendus

- le répertoire config contient les fichiers d'initialisation pouvant être utiles lors du lancement de la plateforme
- le répertoire data accueille les fichiers de données de l'application
- le répertoire doc accueille les rapports attendus
- le répertoire src contient les codes sources. Ce répertoire contient lui-même les sous-répertoires suivants
  - application, pour le code des applications
  - config, pour les utilitaires de configuration
  - formats, pour la spécification et la réalisation des formats
  - hdfs, pour la mise en œuvre de hdfs
  - ordo pour l'ordonnancement et le contrôle des tâches Map/Reduce

##### Configuration de Hidoop
2 variables système sont nécesssaires : 
- **HIDOOP_CLASSES** = localisation des classes java de l'application
- **HIDOOP_HOME** = localisation des fichiers de l'application, idem que dans config.Projet.PATH
  <span style="color:green">*Commande : **export VARIABLE=path/to/folder***</span>

La configuration se fait via un fichier *conf.xml* placé dans le répertoire **HIDOOP_HOME/config/**.
Un fichier d'exemple est donné ci-dessous :

    <?xml version="1.0" encoding="UTF-8"?>
    <config metadata="meta">
        <servers>
        <node ip="127.0.0.1"/>
        <node ip="chewie"/>
        <node ip="yoda"/>
        </servers>
    </config>

Les informations sur les serveurs sont chargées dans ***AppData***. 
Informations et métadonnées peuvent être chargées via la méthode *loadConfigAndMeta()*.

##### Script de lancement 
Le script *hidoop.sh* permet de lancer / arrêter automatiquement les serveurs indiqués dans le fichier de configuration *conf.xml* via ssh (machines N7).
Le répertoire utilisé est celui pointé par **HIDOOP_CLASSES**.

On peut lancer n'importe quelle commande, plus les suivantes :  
 - ***start*** pour lancer les serveurs 
 - ***exit*** pour quitter et fermer les serveurs
 - ***hdfs*** raccourci pour java hdfs.HdfsClient, suivi des mêmes arguments 



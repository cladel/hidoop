Projet **HIDOOP**
-------------------------------
Contenu : 
- fichiers sources sauf *config/Project.java*, cette classe servant uniquement à la définition d'une variable statique ***PATH*** indiquant le répertoire des fichiers d'Hidoop.
- Rapports intermédiaires

##### Configuration de Hidoop
La configuration se fait via un fichier *conf.xml* placé dans le répertoire pointé par ***PATH***.
Un fichier d'exemple est donné ci-dessous :

    <?xml version="1.0" encoding="UTF-8"?>
    <config metadata="meta">
        <servers>
        <node ip="127.0.0.1"/>
        <node ip="127.0.0.1"/>
        <node ip="127.0.0.1"/>
        </servers>
    </config>

Les informations sur les serveurs sont chargées dans ***AppData***. 
Informations et métadonnées peuvent être chargées via la méthode *loadConfigAndMeta()*.

##### Script de lancement 
Le script *hidoop.sh* permet de lancer / arrêter automatiquement les serveurs indiqués dans le fichier de configuration *conf.xml* via ssh (machines N7).
Il utilise 2 variables système :
- **HIDOOP_CLASSES** = localisation des classes java de l'application
- **HIDOOP_HOME** = localisation des fichiers de l'application, comme dans config.Projet

On peut lancer les commandes suivantes, en plus de celles utilisant java :  
 - ***start*** pour lancer les serveurs 
 - ***exit*** pour quitter et fermer les serveurs


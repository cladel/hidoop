Projet **HIDOOP**
-------------------------------
Contenu : 
- fichiers sources sauf *config/Project.java*, cette classe servant uniquement à la définition d'une variable statique ***PATH*** indiquant le répertoire des fichiers d'Hidoop.

##### Configuration de Hidoop
La configuration se fait via un fichier *conf.xml* placé dans le répertoire poité par ***PATH***.
Un fichier d'exemple est donné ci-dessous :

    <?xml version="1.0" encoding="UTF-8"?>
    <config metadata="meta">
        <servers>
        <node ip="127.0.0.1"/>
        <node ip="127.0.0.1"/>
        <node ip="127.0.0.1"/>
        </servers>
    </config>

Les informations sur les serveurs sont chargées dans ***Loader.SERVERS_IP***. 
Informations et métadonnées peuvent être chargées via *Loader.loadConfigAndMeta()*.
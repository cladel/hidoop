Plateforme **HIDOOP**
=======================  
![Language Java](https://img.shields.io/badge/Langage-Java-B07219)
![Platform Linux](https://img.shields.io/badge/Platforme-Linux-red)

**Hidoop est une plateforme destinée à l'exécution d'applications distribuées sur des clusters de machines.**

Le fonctionnement est inspiré de celui de [Hadoop](https://hadoop.apache.org/). La plateforme comporte deux modules :
* 📂 **Un système de fichiers distribué** (HDFS)
* 🚀 **Une implémentation du schéma MapReduce** pour le calcul de données massives

Pour commencer 📍
----------------  

### Prérequis
- L'application fonctionne sur Linux et utilise un terminal **bash**
- Toutes les machines doivent être accessibles depuis le client via **SSH**
- L'application utilise **Java** (> 1.8)

### Installation
Pour commencer, définir une variable d'environnement **HIDOOP_HOME** (pointant sur le répertoire qui sera utilisé par hidoop) sur chaque machine où tournera l'application.  
✏ Les classes sont considérées comme étant dans **$HIDOOP_HOME/src** par défaut, mais il est possible de définir une variable d'environnement **HIDOOP_CLASSES** qui indique la localisation des classes java de l'application.

### Configuration de Hidoop

La configuration se fait via un fichier unique *conf.xml* placé dans le répertoire **$HIDOOP_HOME/config/** de la machine cliente.  
Un fichier d'exemple est donné ci-dessous :
```xml  
<?xml version="1.0" encoding="UTF-8"?>  
<config metadata="meta">  
 <default-chunk-size value="64" unit="bytes" /> 
 <servers> 
    <node ip="127.0.0.1"/> 
    <node ip="chewie"/> 
    <node ip="yoda"/> 
 </servers>
</config>  
```  

- **config** : l'attribut obligatoire _metadata_ est le nom du fichier de métadonnées qui sera créé.
  * **default-chunk-size** : élément optionnel précisant la taille de chunk (entier) à utiliser par défaut dans HDFS. Si absent, cette taille est de 64MB.  
    ✏ Les unités de tailles supportées sont bytes, kB, MB, GB (non sensible à la casse). Par simplicité, une unité inconnue a le même effet que bytes.

  *  **servers** : liste des serveurs. L'attribut _ip_ d'un _node_ correspond soit à l'adresse ip de la machine soit à son nom (hostname). Cette liste ne peut pas être vide.

Utilisation de la plateforme 📚
-----------------------  

La plateforme s'utilise via le script ***hidoop***.  
Commande : `$ ./hidoop <ssh_username>`

Au démarrage, ce shell charge le contenu du fichier de configuration *conf.xml*. Il permet d'interagir avec les machines du cluster via SSH.


### Initialisation et déploiement
Les commandes sont lancées sur la machine cliente après avoir cloné le projet dans **$HIDOOP_HOME** et créé le fichier de configuration.  
On utilise le shell `hidoop-deploy` (accessible via la commandes `deploy`) qui permet d'automatiser le déploiement de la plateforme.

```shell  
$ ./$HIDOOP_HOME/hidoop $USER
hidoop> deploy  
hidoop-deploy> mkhome ; tonodes $NODES ; compile 
hidoop-deploy> exit  
hidoop>  
```  

Ici, on lance l'application en utilisant le nom d'utilisateur courant pour les connections en SSH.  
Dans le shell de déploiement, on commence par créer si nécessaire le dossier défini dans **HIDOOP_HOME** sur les serveurs distants,  puis on envoie l'application sur l'ensemble des serveurs. Enfin on compile l'application sur la machine cliente locale.

#### Detail des commandes

- `compile` pour compiler l'application localement
- `tonode <server1> [ <server2> ... ]` pour déployer sur les serveurs distants passés en argument
- `mkhome` pour créer les répertoires **$HIDOOP_HOME** sur les serveurs où ils n'existent pas
- `rmhome <server1> [ <server2> ... ]` pour effacer le répertoire **$HIDOOP_HOME** et son contenu (données, fichiers de log, etc.) sur les serveurs passés en argument (utile pour supprimer définitivement l'application d'une machine)
- `rmnodedata <server1> [ <server2> ... ]` pour effacer seulement les données des serveurs passés en argument

✏ La variable en lecture seule **$NODES** contient la liste des serveurs configurés dans *conf.xml*.

### Lancement et arrêt de la plateforme

Dans le shell de l'application `hidoop`, il est possible de lancer les commandes suivantes :
- `printconf` pour afficher le contenu de *conf.xml*
- `start` pour lancer la plateforme
- `stop` pour arrêter la plateforme
- `restart` pour recharger la liste des serveurs définis dans *conf.xml* et relancer la plateforme
- `exit` pour sortir du shell et arrêter la plateforme

⚠ En cas de changement de configuration, seule la commande `restart` permet de recharger le contenu du fichier.

### Gestion des fichiers dans HDFS
⚠ Les commandes suivantes nécessitent que la plateforme soit en fonctionnement.
- `hdfs -l [options]` pour lister les fichiers écrits dans HDFS.  
    Options :
    * `--detail` pour des informations sur les chunks

- `hdfs -w <localfilename> [options]` pour écrire un fichier dans HDFS  
  `<localfilename>` peut être un chemin absolu ou relatif à **$HIDOOP_HOME/data/**.  
  Le fichier écrit dans HDFS aura le nom du fichier local.  
    Options :
    * `-f ln|kv` format du fichier (ln par défaut)
    * `--chunks-size=<taille>` taille des chunks (ex : 100MB, 3kB...). Si aucune unité (B, kB, MB, GB, TB) n'est fournie, la valeur est en bytes. Si la valeur est négative, cet argument est ignoré.
    * `--rep=<factor>` facteur de réplication des chunks (entier positif)

- `hdfs -r <filename> [options]` pour lire un fichier stocké dans HDFS vers la machine locale.  
    Options :
    * `<localfilename>` fichier local de destination. Le chemin peut être absolu ou relatif à **$HIDOOP_HOME/data/**. Le fichier est lu dans *r_&lt;filename&gt;* par défaut.

- `hdfs -d <filename>` pour supprimer un fichier de HDFS

#### Format des fichiers

HDFS supporte 2 formats : ***Key-Value*** (*kv*) et ***Line*** (*ln*).

- ***Line*** :

  > Ce fichier est au format ln.    
  > Les lignes ne sont jamais coupées,    
  > mais ne doivent pas être trop longues.

  Dans ce format, les données sont écrites ligne par ligne. Une ligne n'est jamais coupée lors de la fragmentation d'un fichier en chunks.   
  La longueur des lignes ne doit donc pas être trop disparate. Si pour un fichier, une ligne dépasse 2 fois la taille d'un chunk, le fichier ne pourra pas être écrit.



- ***Key-Value*** :

  > ville<->Toulouse    
  > école<->N7    
  > année<->2

  Dans ce format, les données sont écrites avec une paire clé/valeur (séparées par "<->") par ligne. Une ligne n'est jamais coupée lors de la fragmentation d'un fichier en chunks.

### Lancement des applications MapReduce
⚠ Les commandes suivantes nécessitent que la plateforme soit en fonctionnement.

La commande `launch <appname> [options] <filename>` permet de lancer une application de calcul MapReduce.  
Options :
* `-ip <address>` : l'implémentation du MapReduce utilisant Java RMI, il peut être nécessaire de préciser quelle adresse ip transmettre depuis la JVM du client. 

✏ `launch MyMapReduce [options] <filename>` exécute l'application de comptage de mots ***MyMapReduce*** sur un fichier enregistré dans HDFS.  
Le fichier de résultats se nomme *&lt;filename&gt;-tot* et est au format *kv*.


### Supervision et évaluation

Dans le shell `hidoop-monitoring` (accessible via la commandes `monitoring`), on peut utiliser les commandes classiques de l'application et superviser l'application en plus des commandes suivantes :
- `cmpref <file>` pour comparer un résultat de mmr avec sa version séquentielle sur un fichier
- `evalf <fromN> <toN> <file>` pour évaluer les performances sur un fichier selon le nombre de chunks
- `logtail <servername>` pour afficher les dernières lignes de log d'un serveur distant
- `nodels <servername>` pour lister les fichiers de données d'un serveur distant
- `logrm <servername>` pour effacer les logs d'un serveur distant

Le fonctionnement de Hidoop 🖥
--------------------------  

L'application se base sur les interactions entre une machine cliente principale et des serveurs distants qui effectuent le stockage des données et les calculs.

### HDFS
Sur la machine cliente, un daemon *NameNode* tourne en arrière-plan. Il gère la configuration de la plateforme, l'accès aux serveurs et la localisation des chunks des fichiers HDFS (métadonnées).  
Le client communique avec le *NameNode* localement via RMI.

Les requêtes HDFS utilisent des Sockets TCP pour communiquer avec les serveurs désignés par le *NameNode*.

Lorsqu'un fichier est ajouté à HDFS, il est découpé en morceaux (chunks) et réparti sur les différents serveurs disponibles.

### Applications MapReduce

Les applications de MapReduce utilisent RMI avec les serveurs distants pour lancer les calculs sur chaque chunk (phase Map) puis récupèrent les résultats intermédiaires et les combinent (phase Reduce).

### Developpement

Thomas Guillaud - 
Axel Grunig -
Nathan Razafimanantsoa -
Chloe Laplagne
package hdfs;


/**
 * TCP commands sent to HdfsServer
 */
public enum Commands {
    HDFS_WRITE("WRT"),
    HDFS_READ("READ"),
    HDFS_DELETE("DEL");
    private final String nom;

    Commands(String name){
        this.nom = name;
    }

    public String toString(){
        return nom;
    }

    public static Commands fromString(String n){
        switch (n){
            case "WRT" : return HDFS_WRITE;
            case "READ" : return HDFS_READ;
            case "DEL" : return HDFS_DELETE;
            default: return null;
        }
    }

}

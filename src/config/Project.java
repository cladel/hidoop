package config;

public class Project {
    public static final String PATH = System.getenv("HIDOOP_HOME") ;

    public static String getDataPath(){
        return PATH+"data/";
    }

    public static String getConfigPath(){
        return PATH+"config/";
    }
}


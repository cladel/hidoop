package hdfs;

import java.io.*;
import java.util.Date;
import java.util.HashMap;

public class Metadata implements Serializable {
    public static final long versionID = 1L;


    private Date saveDate;
    private final HashMap<String, FileData> files;

    public static Metadata load(File f) throws IOException, ClassNotFoundException {
        Metadata metadata;
        if (f.exists()) {
            FileInputStream file = new FileInputStream(f);
            ObjectInputStream in = new ObjectInputStream(file);

            metadata = (Metadata) in.readObject();
            in.close();
            file.close();
        } else {
            System.out.println("No metadata file found. Running init...");
            metadata = new Metadata();
        }
        return metadata;
    }

    public static void save(File f, Metadata data) throws IOException {
        data.saveDate = new Date();
        FileOutputStream file = new FileOutputStream(f);
        ObjectOutputStream in = new ObjectOutputStream(file);


        in.writeObject(data);
        in.close();
        file.close();
    }

    public Metadata(){
        this.saveDate = new Date();
        this.files = new HashMap<>();
    }

    public void addFileData(String name, FileData fd){
        files.put(name, fd);
    }

    public void removeFileData(String name){
        files.remove(name);
    }

    public FileData retrieveFileData(String name){
        return files.get(name);
    }

    public Date getSaveDate() {
        return saveDate;
    }
}

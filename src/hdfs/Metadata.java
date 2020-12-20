package hdfs;

import java.io.*;
import java.util.*;

/**
 * HDFS metadata, list of data for all the files currently stored
 * in the file system
 */
public class Metadata implements Serializable {
    public static final long serialVersionUID = 1L;


    private Date saveDate;
    private final HashMap<String, FileData> files;

    public static Metadata load(File f, boolean createIfNotFound) throws IOException, ClassNotFoundException {
        Metadata metadata;
        if (f.exists()) {
            FileInputStream file = new FileInputStream(f);
            ObjectInputStream in = new ObjectInputStream(file);

            metadata = (Metadata) in.readObject();
            in.close();
            file.close();
        } else if (createIfNotFound){
            System.out.println("No metadata file found. Creating new file...");
            metadata = new Metadata();
        } else {
            throw new FileNotFoundException("No metadata file found.");
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

    /**
     * Add file data
     * @param name name of the file ( <= 50 chars)
     * @param fd file data
     */
    public void addFileData(String name, FileData fd){
        assert name.length() <= 80; //TODO
        files.put(name, fd);
    }

    public void removeFileData(String name){
        files.remove(name);
    }

    public FileData retrieveFileData(String name){
        return files.get(name);
    }


    public int getFileCount(){
        return files.size();
    }

    public Set<String> getFileNames() {
        return files.keySet();
    }

    public Date getSaveDate() {
        return saveDate;
    }
}

package config;

import hdfs.Constants;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;


/**
 * Loader for Hidoop configuration and metadata
 */
public class AppData extends UnicastRemoteObject implements NameNode {

    // Object containing information about the files stored in Hdfs
    private Metadata metadata;
    // Information about registered servers
    private String[] serversIp;
    // Metadata file name
    private final String DATAFILE_NAME;
    // Default chunk size
    private long defaultChunkSize =  64 * 1000 * 1000; // 64 MB


    private AppData(String datafile) throws RemoteException {
        DATAFILE_NAME = datafile;
    }


    /**
     * Loads configuration (servers nodes and metadata) into memory
     * @param createIfNotFound create metadata file if not found
     * @return Metadata information (since we always need them)
     */
    public static AppData loadConfigAndMeta(boolean createIfNotFound) throws IOException, SAXException,
            ParserConfigurationException, ClassNotFoundException {

        if (Project.PATH == null) {
            throw new IllegalStateException("HIDOOP_HOME is undefined.");
        }
        DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        File fileXML = new File(Project.getConfigPath()+"conf.xml");
        if (fileXML.exists()) {
            // If file exists parse xml
            Document xml = builder.parse(fileXML);

            Element root = xml.getDocumentElement();
            AppData ld = new AppData(root.getAttribute("metadata"));

            NodeList nlist;
            nlist = root.getElementsByTagName("node");
            int l = nlist.getLength();
            if (l == 0) throw new IllegalStateException("Server list cannot be empty.");
            ld.serversIp = new String[l];
            for (int i = 0; i < l; i++) {
                Element e = (Element) nlist.item(i);
                ld.serversIp[i] = e.getAttribute("ip");
            }

            nlist = root.getElementsByTagName("default-chunk-size");

            if (nlist.getLength() == 1){
                Element e = (Element) nlist.item(0);

                float size = Float.parseFloat(e.getAttribute("value"));
                long s = Constants.getSize(size, e.getAttribute("unit"));
                ld.defaultChunkSize = (s < 0) ? (long)size : s;

            }

            ld.metadata = Metadata.load(new File(Project.getConfigPath() + ld.DATAFILE_NAME), createIfNotFound);

            return ld;
        } else {
            throw new FileNotFoundException("No conf.xml found in "+Project.getConfigPath()+".");
        }
    }

    /**
     * Save updated metadata
     */
    private void saveMetadata() {
        try {
            Metadata.save(new File(Project.getConfigPath() + DATAFILE_NAME),metadata);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * @return list of available servers ip
     */
    public String[] getServersIp() {
        return serversIp;
    }


    /**
     * @return defaultChunkSize
     */
    public long getDefaultChunkSize() {
        return defaultChunkSize;
    }

    /**
     * Add file data
     * @param name name of the file
     * @param fd file data
     */
    public void addFileData(String name, FileData fd){
        metadata.files.put(name, fd);
        saveMetadata();
    }

    public void removeFileData(String name){
        metadata.files.remove(name);
        saveMetadata();
    }

    public FileData retrieveFileData(String name){
        return metadata.files.get(name);
    }


    public int getFileCount(){
        return metadata.files.size();
    }

    public List<String> getFileNames() {
        return new ArrayList<>(metadata.files.keySet());
    }

    public Date getSaveDate() {
        return metadata.saveDate;
    }

    public static void main(String[] args){
        try {
            NameNode nm = loadConfigAndMeta(true);
            LocateRegistry.createRegistry(PORT);
            Naming.rebind("//localhost:" + PORT + "/namenode", nm);

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }


    private static class Metadata implements Serializable {
        public static final long serialVersionUID = 1L;

        private Date saveDate;
        private final HashMap<String, FileData> files;

        private static Metadata load(File f, boolean createIfNotFound) throws IOException, ClassNotFoundException {
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

        private static void save(File f, Metadata data) throws IOException {
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


    }

}

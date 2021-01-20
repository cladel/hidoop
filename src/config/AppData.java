package config;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;


/**
 * Loader for Hidoop configuration and metadata
 */
public class AppData {

    // Object containing information about the files stored in Hdfs
    private Metadata metadata;
    // Information about registered servers
    private String[] serversIp;
    // Metadata file name
    private final String DATAFILE_NAME;
    // Default chunk size
    private long defaultChunkSize =  64 * 1024 * 1024; // 64 MB


    private AppData(String datafile) {
        DATAFILE_NAME = datafile;
    }


    /**
     * Loads configuration (servers nodes and metadata) into memory
     * @param createIfNotFound create metadata file if not found
     * @return Metadata information (since we always need them)
     */
    public static AppData loadConfigAndMeta(boolean createIfNotFound) throws IOException, SAXException, ParserConfigurationException, ClassNotFoundException {

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

            if (nlist.getLength() > 0){
                Element e = (Element) nlist.item(0);
                ld.defaultChunkSize = Long.parseLong(e.getAttribute("value"));

                // Adjusting value to unit : if unit is 'bytes' or unknown then use bytes
                switch (e.getAttribute("unit").toUpperCase()){

                    case "GB":
                        ld.defaultChunkSize *= 1024;
                    case "MB":
                        ld.defaultChunkSize *= 1024;
                    case "KB":
                        ld.defaultChunkSize *= 1024;
                        break;

                }
            }

            ld.metadata = Metadata.load(new File(Project.getConfigPath() + ld.DATAFILE_NAME), createIfNotFound);

            return ld;
        } else {
            throw new FileNotFoundException("No conf.xml found in "+Project.getConfigPath()+".");
        }
    }

    /**
     * Save updated metadata
     * @param data metadata object
     */
    public void saveMetadata(Metadata data) throws IOException {
        Metadata.save(new File(Project.getConfigPath() + DATAFILE_NAME),data);
    }


    /**
     * @return list of available servers ip
     */
    public String[] getServersIp() {
        return serversIp;
    }

    /**
     * @return metadata
     */
    public Metadata getMetadata() {
        return metadata;
    }

    /**
     *
     * @return defaultChunkSize
     */
    public long getDefaultChunkSize() {
        return defaultChunkSize;
    }
}

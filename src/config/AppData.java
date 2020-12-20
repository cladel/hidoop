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

    private AppData(String datafile) {
        DATAFILE_NAME = datafile;
    }


    /**
     * Loads configuration (servers nodes and metadata) into memory
     * @param createIfNotFound create metadata file if not found
     * @return Metadata informations (since we always need them)
     */
    public static AppData loadConfigAndMeta(boolean createIfNotFound) throws IOException, SAXException, ParserConfigurationException, ClassNotFoundException {

        DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        File fileXML = new File(Project.PATH+"conf.xml");
        if (fileXML.exists()) {
            Document xml = builder.parse(fileXML);

            Element root = xml.getDocumentElement();
            AppData ld = new AppData(root.getAttribute("metadata"));

            NodeList nlist = root.getElementsByTagName("node");
            int l = nlist.getLength();
            ld.serversIp = new String[l];
            for (int i = 0; i < l; i++) {
                Element e = (Element) nlist.item(i);
                ld.serversIp[i] = e.getAttribute("ip");
            }

            ld.metadata = Metadata.load(new File(Project.PATH + ld.DATAFILE_NAME), createIfNotFound);

            return ld;
        } else {
            throw new FileNotFoundException("No conf.xml found.");
        }
    }

    /**
     * Save updated metadata
     * @param data metadata object
     */
    public void saveMetadata(Metadata data) throws IOException {
        Metadata.save(new File(Project.PATH + DATAFILE_NAME),data);
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
}

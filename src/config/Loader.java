package config;

import hdfs.Metadata;
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
public class Loader {

    private static String[] SERVERS_IP; // Liste des ip des HdfsServer
    private static String DATAFILE_NAME; // Metadata file


    /**
     * Loads configuration (servers nodes and metadata) into memory
     * @return Metadata informations (since we always need them)
     */
    public static Metadata loadConfigAndMeta() throws IOException, SAXException, ParserConfigurationException, ClassNotFoundException {

        DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        File fileXML = new File(Project.PATH+"conf.xml");
        if (fileXML.exists()) {
            Document xml = builder.parse(fileXML);

            Element root = xml.getDocumentElement();
            DATAFILE_NAME = root.getAttribute("metadata");

            NodeList nlist = root.getElementsByTagName("node");
            int l = nlist.getLength();
            SERVERS_IP = new String[l];
            for (int i = 0; i < l; i++) {
                Element e = (Element) nlist.item(i);
                SERVERS_IP[i] = e.getAttribute("ip");
            }

            return Metadata.load(new File(Project.PATH + DATAFILE_NAME));
        } else {
            throw new FileNotFoundException("No conf.xml found.");
        }
    }

    /**
     * @return list of available servers ip
     */
    public static String[] getServersIp() {
        return SERVERS_IP;
    }

    /**
     * @return metadata filename
     */
    public static String getDatafileName() {
        return DATAFILE_NAME;
    }
}

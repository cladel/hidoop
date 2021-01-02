package ordo;

import config.AppData;
import config.FileData;
import config.Metadata;
import formats.*;
import hdfs.HdfsClient;
import map.MapReduce;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.concurrent.ExecutionException;

public class Job implements JobInterface{
    Format.Type fType;
    String fName;

    static public int PORT = 2000;
    //static String server[] = {"Noeud1", "Noeud2", "Noeud3"};
    //static int port[] = {2001, 2002, 2003};

    public Job(){ }

    @Override
    public void setInputFormat(Format.Type ft) {
        this.fType = ft;
    }

    @Override
    public void setInputFname(String fname) {
        this.fName = fname;
    }

    @Override
    public void startJob(MapReduce mr) {

        try {
            AppData m = AppData.loadConfigAndMeta(false);
            String [] server = m.getServersIp();
            Metadata data = m.getMetadata();

            CallBackImpl cb = new CallBackImpl(server.length);

            for (int i=0 ; i<server.length ; i++ ){
                FileData fd = new FileData(fType, FileData.UNKNOWN_SIZE, FileData.UNKNOWN_SIZE);
                data.addFileData(fName+"-res", fd);
                Thread t = new Thread(new Employe(server[i], i, mr, this.fType, this.fName, cb));
                t.start();
                m.saveMetadata(data);
            }

            HdfsClient.HdfsRead(fName+"-res", fName + "-res");

            Format frReduce;
            Format fwReduce;
            if (fType.equals(Format.Type.LINE)){ // détection du type de format d'input,
                                                 // l'output est obligatoirement le même
                frReduce = new KVFormat(fName + "-res");
                fwReduce = new KVFormat(fName + "-tot");
            } else {
                frReduce = new KVFormat(fName + "-res");
                fwReduce = new KVFormat(fName + "-tot");
            }

            cb.attente();
            frReduce.open(Format.OpenMode.R);
            fwReduce.open(Format.OpenMode.W);
            mr.reduce(frReduce, fwReduce);
            frReduce.close();
            fwReduce.close();
            System.out.println("Fini Reduce");

            HdfsClient.HdfsDelete(fName + "-res");
        } catch (InterruptedException | RemoteException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}

class Employe implements Runnable{
    private final CallBackImpl cb;
    private final int numServ;
    private final MapReduce mr;
    private Format.Type fType;
    private String fName;
    private String server;


    public Employe(String server, int nb, MapReduce mr, Format.Type fType, String fName, CallBackImpl cb){
        this.numServ = nb;
        this.mr = mr;
        this.fType = fType;
        this.fName = fName;
        this.cb = cb;
        this.server = server;
    }

    @Override
    public void run() {
        try {
            // Création des formats d'input (fr) et d'output (fw)
            Format frMap;
            Format fwMap;
            if (fType.equals(Format.Type.LINE)){ // détection du type de format d'input,
                                                 // l'output est obligatoirement l'autre
                frMap = new LineFormat(FileData.chunkName(numServ, fName, Format.Type.LINE));
                fwMap = new KVFormat(FileData.chunkName(numServ, fName+"-res", Format.Type.KV));
            } else {
                frMap = new KVFormat(FileData.chunkName(numServ, fName, Format.Type.KV));
                fwMap = new LineFormat(FileData.chunkName(numServ, fName+"-res", Format.Type.LINE));
            }

           // System.out.println("//"+server+":" + Job.PORT + "/" + server);
            Worker worker = (Worker) Naming.lookup("//"+server+":" + Job.PORT + "/worker");
            worker.runMap(mr,frMap,fwMap,cb);
        } catch (NotBoundException exception) {
            exception.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }
}

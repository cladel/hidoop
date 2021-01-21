package ordo;

import config.*;
import formats.*;
import hdfs.HdfsClient;
import map.MapReduce;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.concurrent.ExecutionException;

public class Job implements JobInterface{
    Format.Type fType;
    String fName;

    public Job(){ }

    @Override
    public void setInputFormat(Format.Type ft) {
        this.fType = ft;
    }

    public Format.Type getInputFormat(){return this.fType;}

    @Override
    public void setInputFname(String fname) {
        this.fName = fname;
    }

    public String getInputFname(){return this.fName;}

    @Override
    public void startJob(MapReduce mr) {

        try {
            // Recuperer les metadonnées et les ip des serveurs
            AppData m = AppData.loadConfigAndMeta(false);
            String [] server = m.getServersIp();
            Metadata data = m.getMetadata();

            CallBackImpl cb = new CallBackImpl(server.length);
            Format.Type ft;
            FileData fd;

            if (fType.equals(Format.Type.LINE)) ft = Format.Type.KV;
            else ft = Format.Type.LINE;

            // Creation d'un FileData pour le fichier des résultats de map, sans spécifier sa taille
            fd = new FileData(ft);

            // Calculer et répertorier les résultats des maps
            for (int i=0 ; i<server.length ; i++ ){
                Thread t = new Thread(new Employe(server[i], i, mr, this.fType, this.fName, cb));
                t.start();
                fd.addChunkHandle(i, server[i]);
            }

            data.addFileData(fName+"-res", fd);
            HdfsClient.useData(m);

            // Attente avant de récupérer les résultats des workers
            cb.attente();

            // Récupération
            HdfsClient.HdfsRead(fName+"-res", fName + "-res");
            File tmp = new File(Project.getDataPath()+fName + "-res");
            if (!tmp.exists()) throw new RuntimeException("Erreur de récupération des chunks.");

            Format frReduce;
            Format fwReduce;
            if (fType.equals(Format.Type.LINE)){ // détection du type de format d'input,
                // l'output est obligatoirement le même
                frReduce = new KVFormat(Project.getDataPath()+fName + "-res");
                fwReduce = new KVFormat(Project.getDataPath()+fName + "-tot");
            } else {
                frReduce = new KVFormat(Project.getDataPath()+fName + "-res");
                fwReduce = new KVFormat(Project.getDataPath()+fName + "-tot");
            }

            frReduce.open(Format.OpenMode.R);
            fwReduce.open(Format.OpenMode.W);
            mr.reduce(frReduce, fwReduce);
            frReduce.close();
            fwReduce.close();
            System.out.println("Fini Reduce");

            HdfsClient.HdfsDelete(fName + "-res");
            tmp.deleteOnExit();

        } catch (InterruptedException | IOException | ParserConfigurationException | SAXException | ClassNotFoundException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}

class Employe implements Runnable{
    private final CallBackImpl cb;
    private final int numServ;
    private final MapReduce mr;
    private final Format.Type fType;
    private final String fName;
    private final String server;


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
                frMap = new LineFormat(Project.getDataPath()+FileData.chunkName(numServ, fName, Format.Type.LINE));
                fwMap = new KVFormat(Project.getDataPath()+FileData.chunkName(numServ, fName+"-res", Format.Type.KV));
            } else {
                frMap = new KVFormat(Project.getDataPath()+FileData.chunkName(numServ, fName, Format.Type.KV));
                fwMap = new LineFormat(Project.getDataPath()+FileData.chunkName(numServ, fName+"-res", Format.Type.LINE));
            }
            Worker worker = (Worker) Naming.lookup("//"+server+":" + WorkerImpl.PORT + "/worker");
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

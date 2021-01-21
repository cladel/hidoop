package ordo;

import config.*;
import formats.*;
import hdfs.HdfsClient;
import map.MapReduce;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class Job implements JobInterface{
    Format.Type fType;
    String fName;

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
            // Recuperer les metadonnées et les ip des serveurs
            AppData m = AppData.loadConfigAndMeta(false);
            Metadata data = m.getMetadata();

            Format.Type ft;

            if (fType.equals(Format.Type.LINE)) ft = Format.Type.KV;
            else ft = Format.Type.LINE;

            // Fichier dont on veut les chunks
            FileData fd = data.retrieveFileData(fName);
            if (fd == null) throw new FileNotFoundException(fName);
            // Fichier temporaire pour les résultats du map
            FileData newfd = new FileData(ft);

            // Creation d'un FileData pour le fichier des résultats de map, sans spécifier sa taille


            List<Integer> chunkList = fd.getChunksIds();

            CallBackImpl cb = new CallBackImpl(chunkList.size());

            System.out.println("Launching workers...");
            // Calculer et répertorier les résultats des maps
            String server;
            for (int i : chunkList){
                server = fd.getSourcesForChunk(i).get(0); // tjs rep = 1
                Thread t = new Thread(new Employe(server, i, mr, this.fType, this.fName, cb));
                t.start();
                newfd.addChunkHandle(i, server);
            }

            data.addFileData(fName+"-res", newfd);
            HdfsClient.useData(m);

            // Attente avant de récupérer les résultats des workers
            cb.attente();

            // Récupération
            HdfsClient.HdfsRead(fName+"-res", fName + "-res");
            File tmp = new File(Project.getDataPath()+fName + "-res");
            if (!tmp.exists()) throw new RuntimeException("Erreur de récupération des chunks.");
            System.out.println("Launching reduce task...");

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
            System.out.println("Reduce done : "+fName+"-tot");

            // Delete à la toute fin
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    HdfsClient.HdfsDelete(fName + "-res");
                } catch (IOException | InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }));
            tmp.deleteOnExit();

        } catch (InterruptedException | IOException | ParserConfigurationException | SAXException | ClassNotFoundException | ExecutionException e) {
            System.out.println(e.getMessage());
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
                frMap = new LineFormat(FileData.chunkName(numServ, fName, Format.Type.LINE));
                fwMap = new KVFormat(FileData.chunkName(numServ, fName+"-res", Format.Type.KV));
            } else {
                frMap = new KVFormat(FileData.chunkName(numServ, fName, Format.Type.KV));
                fwMap = new LineFormat(FileData.chunkName(numServ, fName+"-res", Format.Type.LINE));
            }

            Worker worker = (Worker) Naming.lookup("//"+server+":" + WorkerImpl.PORT + "/worker");
            worker.runMap(mr,frMap,fwMap,cb);
        } catch (NotBoundException | MalformedURLException | RemoteException exception) {
            System.out.println(exception.getMessage());
            exception.printStackTrace();
        }
    }
}

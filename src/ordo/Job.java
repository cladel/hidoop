package ordo;

import config.*;
import formats.*;
import hdfs.Deleter;
import hdfs.Reader;
import map.MapReduce;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

    public Format.Type getInputFormat(){return this.fType;}


    public String getInputFname(){return this.fName;}


    @Override
    public void startJob(MapReduce mr) {

        try {
            // Recuperer les metadonnées et les ip des serveurs
            NameNode data = (NameNode) Naming.lookup("//localhost:" + NameNode.PORT + "/namenode");

            Format.Type ft;

            if (fType.equals(Format.Type.LINE)) ft = Format.Type.KV;
            else ft = Format.Type.LINE;

            // Fichier dont on veut les chunks
            FileData fd = data.retrieveFileData(fName);
            if (fd == null) throw new FileNotFoundException(fName);
            // Creation d'un FileData pour le fichier temporaire des résultats de map, sans spécifier sa taille
            final FileData newfd = new FileData(ft);


            List<Integer> chunkList = fd.getChunksIds();

            CallBackImpl cb = new CallBackImpl(chunkList.size());

            System.out.println("Launching workers...");

            // Calculer et répertorier les résultats des maps

            String server;
            ExecutorService pool = Executors.newCachedThreadPool();
            for (int i : chunkList){
                server = fd.getSourcesForChunk(i).get(0); // tjs rep = 1

                pool.submit(new Employe(server, i, mr, this.fType, this.fName, cb));
                newfd.addChunkHandle(i, server);
            }


            // Attente avant de récupérer les résultats des workers
            cb.attente();
            pool.shutdown();

            // Récupération
            System.out.println("Reading map results...");
            File tmp = new File(Project.getDataPath()+fName + "-res");
            Reader reader = new Reader(newfd, fName+"-res", tmp);
            if (!reader.exec()) throw new RuntimeException();
            if (!tmp.exists()) throw new IOException("Erreur de récupération des chunks.");
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
               Deleter del = new Deleter(newfd, fName+"-res");
               del.setPrintProgress(false);
               del.exec();

            }));
            tmp.deleteOnExit();

        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}


class Employe implements Runnable {
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
            Format frMap = new LineFormat(FileData.chunkName(numServ, fName, fType));
            Format fwMap = new KVFormat(FileData.chunkName(numServ, fName+"-res", fType == Format.Type.LINE ? Format.Type.KV : Format.Type.LINE));

            Worker worker = (Worker) Naming.lookup("//"+server+":" + WorkerImpl.PORT + "/worker");
            worker.runMap(mr,frMap,fwMap,cb);
        } catch (NotBoundException | MalformedURLException | RemoteException exception) {
            System.out.println(exception.getMessage());
            exception.printStackTrace();
        }
    }
}

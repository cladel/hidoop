package ordo;

import formats.*;
import map.MapReduce;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

public class Job implements JobInterface{
    Format.Type fType;
    String fName;
    static String server[] = {"Noeud1", "Noeud2", "Noeud3"};
    static int port[] = {2001, 2002, 2003};

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
            CallBackImpl cb = new CallBackImpl(server.length);

            for (int i=0 ; i<server.length ; i++ ){
                Thread t = new Thread(new Employe(i, mr, this.fType, this.fName, cb));
                t.start();
            }

            Format frReduce;
            Format fwReduce;
            if (fType.equals(Format.Type.LINE)){ // détection du type de format d'input,
                                                 // l'output est obligatoirement l'autre
                frReduce = new KVFormat(fName + "-tot");
                fwReduce = new KVFormat(fName + "-res");
            } else {
                frReduce = new KVFormat(fName + "-tot");
                fwReduce = new KVFormat(fName + "-res");
            }

            cb.attente();
            frReduce.open(Format.OpenMode.R);
            fwReduce.open(Format.OpenMode.W);
            mr.reduce(frReduce, fwReduce);
            frReduce.close();
            fwReduce.close();
            System.out.println("Fini Reduce");
        } catch (InterruptedException | RemoteException e) {
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


    public Employe(int nb, MapReduce mr, Format.Type fType, String fName, CallBackImpl cb){
        this.numServ = nb;
        this.mr = mr;
        this.fType = fType;
        this.fName = fName;
        this.cb = cb;
    }

    @Override
    public void run() {
        try {
            // Création des formats d'input (fr) et d'output (fw)
            Format frMap;
            Format fwMap;
            if (fType.equals(Format.Type.LINE)){ // détection du type de format d'input,
                                                 // l'output est obligatoirement l'autre
                frMap = new LineFormat(fName + "-" + (numServ+1));
                fwMap = new KVFormat(fName + "-" + (numServ+1) + "res");
            } else {
                frMap = new KVFormat(fName + "-" + (numServ+1));
                fwMap = new LineFormat(fName + "-" + (numServ+1) + "res");
            }

            System.out.println("//localhost:" + Job.port[numServ] + "/" + Job.server[numServ]);
            Worker worker = (Worker) Naming.lookup("//localhost:" + Job.port[numServ] + "/" + Job.server[numServ]);
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

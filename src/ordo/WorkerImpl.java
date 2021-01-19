package ordo;

import formats.Format;
import map.Mapper;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;

public class WorkerImpl extends UnicastRemoteObject implements Worker {

    static public int PORT = 2000;


    protected WorkerImpl() throws RemoteException { }

    @Override
    public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {
        Thread t = new Thread(new RunMap(m, reader, writer, cb));
        t.start();
    }

    public static void main(String args[]){
        try {
            WorkerImpl worker = new WorkerImpl();
            LocateRegistry.createRegistry(PORT);
            Naming.rebind("//localhost:" + PORT + "/worker", worker);
        } catch (RemoteException | MalformedURLException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}

class RunMap implements Runnable {
    private final Mapper map;
    private final Format reader;
    private final Format writer;
    private final CallBack cb;

    public RunMap(Mapper m, Format reader, Format writer, CallBack cb){
        this.map = m;
        this.reader = reader;
        this.writer = writer;
        this.cb = cb;
    }

    @Override
    public void run() {
        reader.open(Format.OpenMode.R);
        writer.open(Format.OpenMode.W);
        map.map(reader, writer);
        reader.close();
        writer.close();
        System.out.println("Fini Map");
        try {
            cb.reveiller();
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }
}
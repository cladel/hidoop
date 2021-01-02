package ordo;

import formats.Format;
import map.Mapper;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;

public class WorkerImpl extends UnicastRemoteObject implements Worker {


    protected WorkerImpl() throws RemoteException { }

    @Override
    public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {
        reader.open(Format.OpenMode.R);
        writer.open(Format.OpenMode.W);
        m.map(reader, writer);
        reader.close();
        writer.close();
        System.out.println("Fini Map");
        cb.reveiller();
    }

    public static void main(String args[]){
        try {
          //  choix = Integer.parseInt(args[0]) - 1;
            WorkerImpl worker = new WorkerImpl();
            LocateRegistry.createRegistry(Job.PORT);
            Naming.rebind("//localhost:" + Job.PORT + "/worker", worker);
        } catch (RemoteException | MalformedURLException e) {
            e.printStackTrace();
        }
    }
}

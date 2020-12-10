package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface CallBack extends Remote {
    public void attente() throws InterruptedException, RemoteException;
    public void reveiller() throws RemoteException;
}

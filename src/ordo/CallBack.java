package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.concurrent.TimeoutException;

public interface CallBack extends Remote {
    public void attente() throws TimeoutException, InterruptedException, RemoteException;
    public void reveiller() throws RemoteException;
}

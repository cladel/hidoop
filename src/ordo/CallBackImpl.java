package ordo;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class CallBackImpl extends UnicastRemoteObject implements CallBack {

    private final ReentrantLock moniteur;
    private final Condition jobAttente;
    private final int nbNoeuds;
    private final Condition noeudAttente;
    private final static long ATTENTE = 120;

    public CallBackImpl(int nbNoeuds) throws RemoteException {
        this.nbNoeuds = nbNoeuds;
        this.moniteur = new ReentrantLock();
        this.jobAttente = this.moniteur.newCondition();
        this.noeudAttente = this.moniteur.newCondition();
    }

    public void attente() throws InterruptedException, TimeoutException {
        moniteur.lock();
        int i = nbNoeuds;
        boolean timeok = true;
        System.out.print("\r"+i + " signaux en attente...");
        while (i > 0 && timeok){
            noeudAttente.signal();
            timeok = jobAttente.await(ATTENTE, TimeUnit.SECONDS);
            i--;
            System.out.print("\r"+i + " signaux en attente...");
        }
        System.out.println();
        if (i==0) {
            System.out.println("Fin Attente");
        } else {
            throw new TimeoutException("Connection timed out."); // Utiliser TimeoutException ?
        }
        moniteur.unlock();
    }

    public void reveiller() throws RemoteException {
        moniteur.lock();
        while (!moniteur.hasWaiters(jobAttente)){
            try {
                noeudAttente.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        jobAttente.signal();
        moniteur.unlock();
    }
}

package ordo;

import java.rmi.Remote;
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
    private final static long tempAttente = 30; // temps en secondes, correspond au temps d'attente de fin d'un worker avant de déclarer qu'il y a un problème

    public CallBackImpl(int nbNoeuds) throws RemoteException {
        this.nbNoeuds = nbNoeuds;
        this.moniteur = new ReentrantLock();
        this.jobAttente = this.moniteur.newCondition();
        this.noeudAttente = this.moniteur.newCondition();
    }

    public void attente() throws TimeoutException, RemoteException, InterruptedException {
        moniteur.lock();
        int i = nbNoeuds;
        boolean timeok = true;
        while (i > 0 && timeok){
            System.out.println(i + " Signaux en attente");
            noeudAttente.signal();
            timeok = jobAttente.await(tempAttente, TimeUnit.SECONDS);
            if (timeok) {
                i--;
            }
        }
        if (i==0) {
            System.out.println("Fin Attente");
        } else {
            System.out.println();
            throw new TimeoutException("Server not responding...");
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

package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class CallBackImpl extends UnicastRemoteObject implements CallBack {

    private final ReentrantLock moniteur;
    private final Condition signal;
    private final int nbNoeuds;
    private final Condition noeudAttente;

    public CallBackImpl(int nbNoeuds) throws RemoteException {
        this.nbNoeuds = nbNoeuds;
        this.moniteur = new ReentrantLock();
        this.signal = this.moniteur.newCondition();
        this.noeudAttente = this.moniteur.newCondition();
    }

    public void attente() throws InterruptedException {
        moniteur.lock();
        int i = nbNoeuds;
        while (i > 0){
            noeudAttente.signal();
            signal.await();
            i--;
            System.out.println(i + " Signaux en attente");
        }
        System.out.println("Fin Attente");
        moniteur.unlock();
    }

    public void reveiller() throws RemoteException {
        moniteur.lock();
        while (!moniteur.hasWaiters(signal)){
            try {
                noeudAttente.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        signal.signal();
        moniteur.unlock();
    }
}

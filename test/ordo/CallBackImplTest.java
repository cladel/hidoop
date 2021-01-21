package ordo;

import org.junit.Before;
import org.junit.Test;

import java.rmi.RemoteException;
import java.util.concurrent.TimeoutException;

public class CallBackImplTest {

    private CallBack cb;
    private Thread threadAttente;
    private Thread threadReveil1;
    private Thread threadReveil2;
    private Thread threadReveil3;


    @Before
    public void init(){
        try {
            cb = new CallBackImpl(3);
            threadAttente = new Thread(new attente(cb));
            threadReveil1 = new Thread(new reveil(cb));
            threadReveil2 = new Thread(new reveil(cb));
            threadReveil3 = new Thread(new reveil(cb));
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void callbackTestNormal() {
            // Lancement du thread d'attente
            System.out.println("Attente en cours...");
            threadAttente.start();

            // Lancement des threads de réveil
            System.out.println("lancement 1er Réveil");
            threadReveil1.start();
            System.out.println("lancement 2e Réveil");
            threadReveil2.start();
            System.out.println("lancement 3e Réveil");
            threadReveil3.start();

            // Attente de la fin des 3 réveils
            while (threadReveil1.getState() != Thread.State.TERMINATED){}
            while (threadReveil2.getState() != Thread.State.TERMINATED){}
            while (threadReveil3.getState() != Thread.State.TERMINATED){}
            while (threadAttente.getState() != Thread.State.TERMINATED) {}

            if (threadAttente.getState() == Thread.State.TERMINATED){
                assert true;
            } else {
                assert false;
            }
    }

    /** Pour ce test, il doit y avoir une exception du type "TimeoutException: Server not responding..."
     * Il serv à vérifier qu'un callback ne se bloque pas si il y a un problème (délai à vérifier dans le callback) */
    @Test
    public void callbackTestErreur() {
            // Lancement du thread d'attente
            System.out.println("Attente en cours...");
            threadAttente.start();

            // Lancement des threads de réveil
            System.out.println("lancement 1er Réveil");
            threadReveil1.start();
            System.out.println("lancement 2e Réveil");
            threadReveil2.start();
            System.out.println("lancement 3e Réveil");

            // Attente de la fin des 3 réveils
            while (threadReveil1.getState() != Thread.State.TERMINATED){}
            while (threadReveil2.getState() != Thread.State.TERMINATED){}
            while (threadAttente.getState() != Thread.State.TERMINATED) {}


            if (threadAttente.getState() == Thread.State.TERMINATED){
                assert true;
            } else {
                assert false;
            }
    }
}

class attente implements Runnable{
    CallBack cb;

    attente (CallBack cb){
        this.cb = cb;
    }
    @Override
    public void run() {
        try {
            this.cb.attente();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }
}

class reveil implements Runnable{
    CallBack cb;

    reveil (CallBack cb){
        this.cb = cb;
    }
    @Override
    public void run() {
        try {
            this.cb.reveiller();
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }
}
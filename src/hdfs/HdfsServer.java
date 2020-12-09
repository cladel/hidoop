/* une PROPOSITION de squelette, incomplète et adaptable... */

package hdfs;
import config.Project;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;


/** Commande + nom + taille */
public class HdfsServer { 

    /** Variables */
    final static int port = 3000;
    final static int tailleBuff = 1024;
    final static String separator = " ";

    /** A thread for each client */
    private static class Slave extends Thread {
        Socket socket;

        public Slave(Socket socket) {
            this.socket = socket;
        }
        
        public void run() {

            try{
            /** Connection to HdfsClient */
            InputStream inS = socket.getInputStream();
            OutputStream ouS = socket.getOutputStream();

            /** Buffer */
            byte[] buffer = new byte[tailleBuff];
            int nbOctetsInLus = inS.read(buffer);

            /** ToString + Split */
            String message = new String (buffer, StandardCharsets.US_ASCII);

            /** First message : command */
            String[] args = message.trim().split(separator);
            Commands cmd = Commands.fromString(args[0]);
            if (cmd != null) {

                String nameFile;


                /* Execute command */
                switch (cmd) {

                    /* WRITE */
                    case HDFS_WRITE :
                        if (args.length != 3) {
                            System.err.println("Erreur HDFS_WRITE Serveur : 2 arguments attendus");
                        } else {
                            nameFile = args[1];
                            int sizeFile = Integer.parseInt(args[2]);
                            int nbytesTotal = 0;

                            /* Fichier crée */
                            File fichier = new File(Project.PATH + nameFile);
                            FileOutputStream fileStream = new FileOutputStream(fichier);
                            while (nbytesTotal < sizeFile && nbOctetsInLus != -1) {
                                nbOctetsInLus = inS.read(buffer);
                                fileStream.write(buffer, 0, nbOctetsInLus);
                                nbytesTotal += nbOctetsInLus;
                            }
                        }
                        break;
                    
                    /* READ */
                    case HDFS_READ :
                        if (args.length != 2) {
                            System.err.println("Erreur HDFS_READ Serveur : 1 argument attendu");
                        } else {
                            nameFile = args[1];

                            /* Sending file */
                            File fichier = new File(Project.PATH + nameFile);
                            
                            /* File doesn't exist */
                            if (fichier.exists()) {
                                long sizeFile = fichier.length();
                                FileInputStream fileStream = new FileInputStream(fichier);
                                int nbytesTotal = 0;
                                while (nbytesTotal < sizeFile && nbOctetsInLus != -1) {
                                    nbOctetsInLus = fileStream.read(buffer);
                                    ouS.write(buffer, 0, nbOctetsInLus);
                                    nbytesTotal +=nbOctetsInLus;
                                }
                            }
                        }
                        break;

                    /* DELETE */
                    case HDFS_DELETE :
                        if (args.length != 2) {
                            System.err.println("Erreur HDFS_DELETE Serveur : Aucun argument attendu");
                        } else {
                            nameFile = args[1];

                            /* Suppression du fichier */
                            File fichier = new File(Project.PATH + nameFile);

                            if (fichier.exists()) {
                                fichier.delete();
                            }
                        }
                        break;

                    /* Wrong Command */
                    default : System.err.println("Bad message");
                }
            }

        /** Fermeture */
        socket.close();
            } catch (Exception e){
                e.printStackTrace();
            }

        }
    }


    public static void main (String args[]) {
        try {
            ServerSocket server = new ServerSocket(port);
            while (true) {
                Socket socket = server.accept();
                Slave slave = new Slave(socket);
                slave.start();
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    } 
}

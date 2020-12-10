/* une PROPOSITION de squelette, incomplète et adaptable... */

package hdfs;
import config.Project;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;


/** Commande + nom + taille */
public class HdfsServer {

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

            /** Buffer commande */

            byte[] buf = new byte[Constants.CMD_BUFFER_SIZE];
            int nbOctetsInLus = inS.read(buf);

            /** ToString + Split */
            String message = new String (buf, StandardCharsets.UTF_8);

            /** First message : command */
            System.out.println("CMD : "+message);
            String[] args = message.trim().split(Constants.SEPARATOR);
            Commands cmd = Commands.fromString(args[0]);
            if (cmd != null) {

                String nameFile;


                /* Execute command */
                switch (cmd) {

                    /* WRITE */
                    case HDFS_WRITE :
                        if (args.length != 3) {
                            System.err.println("Erreur HDFS_WRITE Serveur : 2 arguments attendus, trouvés "+args.length);
                        } else {
                            nameFile = args[1];
                            int sizeFile = Integer.parseInt(args[2]);
                            int nbytesTotal = 0;
                            /** Buffer */
                            byte[] buffer = new byte[Constants.BUFFER_SIZE];

                            /* Fichier crée */
                            File fichier = new File(Project.PATH + nameFile);
                            FileOutputStream fileStream = new FileOutputStream(fichier);
                            while ((nbOctetsInLus = inS.read(buffer)) != -1) {

                                fileStream.write(buffer, 0, nbOctetsInLus);
                                nbytesTotal += nbOctetsInLus;
                            //    if (nbytesTotal > 100000000) throw new ; //TODO
                            }
                            System.out.println(nameFile +" saved.");
                        }
                        break;
                    
                    /* READ */
                    case HDFS_READ :
                        if (args.length != 2) {
                            System.err.println("Erreur HDFS_READ Serveur : 1 argument attendu");
                        } else {
                            nameFile = args[1];
                            /** Buffer */
                            byte[] buffer = new byte[Constants.BUFFER_SIZE];

                            /* Sending file */
                            File fichier = new File(Project.PATH + nameFile);
                            
                            /* File doesn't exist */
                            if (fichier.exists()) {

                                FileInputStream fileStream = new FileInputStream(fichier);
                                while ((nbOctetsInLus = fileStream.read(buffer)) != -1) {

                                    ouS.write(buffer, 0, nbOctetsInLus);

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
                                System.out.println(nameFile +" deleted.");
                            }
                        }
                        break;

                    /* Wrong Command */
                    default : System.err.println("Bad message: "+cmd);
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
            ServerSocket server = new ServerSocket(Constants.PORT);
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

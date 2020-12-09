/* une PROPOSITION de squelette, incomplète et adaptable... */

package hdfs;
import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;
import java.net.Socket;
import Command;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/** Commande + nom + taille */

public class HdfsServer { 

    /** Variables */
    final static int port = 3000;
    final static int tailleBuff = 1024;
    final static String separator = " ";

    /** A thread for each client */
    class Slave extends Thread {
        Socket socket;

        public Slave(Socket socket) {
            this.socket = socket;
        }
        
        public void run() {

            /** Connection to HdfsClient */
            InputStream inS = socket.getInputStream();
            OutputStream ouS = socket.getOutputStream();

            /** Buffer */
            byte[] buffer = new byte[tailleBuff];
            int nbOctetsInLus = inS.read(buff); 

            /** ToString + Split */
            String message = new String (buffer, StandardCharsets.US_ASCII);

            /** First message : command */
            if (message != null) {
                String[] args = message.split(separator);
                String nameFile;
                int sizeFile;

                /* Execute command */
                switch (fromString(args[0])) {

                    /* WRITE */
                    case Command.HDFS_WRITE :
                        if (args.length != 3) {
                            System.err.println("Erreur HDFS_WRITE Serveur : 2 arguments attendus");
                        } else {
                            nameFile = args[1];
                            sizeFile = Interger.parseInt(args[2]);
                            int nbytesTotal = 0;

                            /* Fichier crée */
                            File fichier = new File(Project.PATH + nameFile);
                            FileOutputStream fileStream = new FileOutputStream(file);
                            while (nbytesTotal < sizeFile && nbOctetsInLus != -1) {
                                nbOctetsInLus = inS.read(buffer);
                                fileStream.write(buffer, 0, nbOctetsInLus);
                                nbytesTotal += nbOctetsInLus;
                            }
                        }
                        break;
                    
                    /* READ */
                    case Command.HDFS_READ :
                        if (args.length != 2) {
                            System.err.println("Erreur HDFS_READ Serveur : 1 argument attendu");
                        } else {
                            nameFile = args[1];
                            int nbytesTotal = 0;

                            /* Sending file */
                            File fichier = new File(Project.PATH + nameFile);
                            
                            /* File doesn't exist */
                            if (fichier.exist() {
                                FileInputStream fileStream = new FileInputStream(fichier);
                                int nbytesTotal = 0;
                                while (nbytesTotal < sizeFile && nbOctetsInLus != -1) {
                                    nbOctetsInLus = fileStream.read(buffer)
                                    ouS.write(buffer, 0, nbOctetsOutLus);
                                    nbytesTotal +=nbOctetsInLus;
                                }
                            }
                        }
                        break;

                    /* DELETE */
                    case Command.HDSFS_DELETE :
                        if (args.length != 2) {
                            System.err.println("Erreur HDFS_DELETE Serveur : Aucun argument attendu");
                        } else {
                            nameFile = args[1];

                            /* Suppression du fichier */
                            File fichier = new File(Project.PATH + nameFile);

                            if (fichier.exist()) {
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

        }
    }


    public static void main (String args[]) {
        ServerSocket server = new ServerSocket(port);
        while(true) {
            Socket socket = server.accept();
            Slave slave = new Slave(socket);
            slave.start();
        }
    } 
}

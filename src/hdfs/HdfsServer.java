package hdfs;
import config.Project;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;


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

            /** Buffer commande : Commande + nom (+ taille) */

            byte[] buf = new byte[Constants.CMD_BUFFER_SIZE];
            inS.readNBytes(buf, 0, Constants.CMD_BUFFER_SIZE);

            /** ToString + Split */
            String message = new String (buf, StandardCharsets.UTF_8);

            /** First message : command */
            System.out.println("CMD : "+message);
            String[] args = message.trim().split(Constants.SEPARATOR);
            Commands cmd = Commands.fromString(args[0]);
            if (cmd != null) {

                String nameFile;
                int nbOctetsInLus;


                /* Execute command */
                switch (cmd) {

                    /* WRITE */
                    case HDFS_WRITE :
                        if (args.length != 3) {
                            System.err.println("Erreur HDFS_WRITE Serveur : 2 arguments attendus, trouvés "+args.length);
                        } else {
                            nameFile = args[1];
                            long minFileSize = Long.parseLong(args[2]);
                            // TODO ici confirmation taille disponible sur le serveur

                            int nbytesTotal = 0;
                            /** Buffer */
                            byte[] buffer = new byte[Constants.BUFFER_SIZE];

                            /* Fichier crée */
                            File fichier = new File(Project.PATH + nameFile);
                            FileOutputStream fileStream = new FileOutputStream(fichier);

                            int end = -1;

                            nbOctetsInLus = inS.read(buffer);
                            while ((nbytesTotal += nbOctetsInLus) < minFileSize || (end = Constants.findByte(buffer, Constants.END_CHUNK_DELIMITER,0,nbOctetsInLus)) == -1){

                                if (nbytesTotal > minFileSize * 5){ // TODO Refuser chunk trop gros (sécurité)
                                    Constants.putLong(buf, Constants.FILE_TOO_LARGE);
                                    ouS.write(buf,0,Long.BYTES);
                                    socket.close();
                                    return;
                                }
                                fileStream.write(buffer, 0, nbOctetsInLus);
                                nbOctetsInLus = inS.read(buffer);
                            }


                            fileStream.write(buffer, 0, end);
                            // Mise à jour de la taille écrite
                            nbytesTotal -= (nbOctetsInLus - end);


                            if (nbytesTotal > 0) {
                                Constants.putLong(buf, nbytesTotal);
                                ouS.write(buf,0,Long.BYTES);
                                System.out.println(nameFile + " saved ("+nbytesTotal+" B).");
                            } else {
                                Constants.putLong(buf, Constants.FILE_EMPTY);
                                ouS.write(buf,0,Long.BYTES);
                                fileStream.close();
                                fichier.delete();
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

                            if (fichier.exists()) {
                                /** Envoi de la taille du chunk */
                                Constants.putLong(buf, fichier.length());
                                ouS.write(buf,0,Long.BYTES);
                                /** Buffer */
                                byte[] buffer = new byte[Constants.BUFFER_SIZE];

                                FileInputStream fileStream = new FileInputStream(fichier);
                                while ((nbOctetsInLus = fileStream.read(buffer)) != -1) {
                                    ouS.write(buffer, 0, nbOctetsInLus);
                                }

                            } else {
                                /* File doesn't exist */
                                Constants.putLong(buf, Constants.FILE_NOT_FOUND);
                                ouS.write(buf,0,Long.BYTES);
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
                                Constants.putLong(buf, 0);
                                ouS.write(buf,0,Long.BYTES);
                            } else {
                                /* File doesn't exist */
                                Constants.putLong(buf, Constants.FILE_NOT_FOUND);
                                ouS.write(buf,0,Long.BYTES);
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

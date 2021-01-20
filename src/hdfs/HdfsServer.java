package hdfs;
import config.Project;

import java.io.*;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


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
            String message = new String (buf, StandardCharsets.UTF_8).trim();

            /** First message : command */
            System.out.println("CMD : "+message);
            String[] args = message.split(Constants.SEPARATOR);
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

                            // Confirmation allocation possible sur le serveur
                            File dir = new File(Project.getDataPath());
                            long usable = dir.getUsableSpace();
                            if (usable <= 2 * minFileSize){
                                System.err.println("Erreur espace insuffisant : "+ usable + " bytes.");
                                Constants.putLong(buf, Constants.FILE_TOO_LARGE);
                                ouS.write(buf,0,Long.BYTES);
                                socket.close();
                                return;
                            } else {
                                Constants.putLong(buf, 0);
                                ouS.write(buf,0,Long.BYTES);
                            }

                            int nbytesTotal = 0;
                            /** Buffer */
                            byte[] buffer = new byte[Constants.BUFFER_SIZE];

                            /* Fichier crée */
                            File fichier = new File(Project.getDataPath() + nameFile);
                            FileOutputStream fileStream = new FileOutputStream(fichier);

                            int end = -1;

                            nbOctetsInLus = inS.read(buffer);
                            while ((nbytesTotal += nbOctetsInLus) < minFileSize || (end = Constants.findByte(buffer, Constants.END_CHUNK_DELIMITER,0,nbOctetsInLus)) == -1){

                                if (nbytesTotal > minFileSize * 2){
                                    // Refuser chunk trop gros (sécurité)
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
                            File fichier = new File(Project.getDataPath() + nameFile);

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
                                System.err.println(fichier.getName()+" not found.");
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
                            File fichier = new File(Project.getDataPath() + nameFile);

                            if (fichier.exists()) {
                                if (fichier.delete()) System.out.println(nameFile +" deleted.");
                                else  System.err.println("Error deleting "+nameFile);
                                Constants.putLong(buf, 0);
                                ouS.write(buf,0,Long.BYTES);
                            } else {
                                /* File doesn't exist */
                                Constants.putLong(buf, Constants.FILE_NOT_FOUND);
                                ouS.write(buf,0,Long.BYTES);
                                System.err.println(fichier.getName()+" not found.");
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
            // Indiquer le démarrage pour les log
            System.out.println("HDFS start : "+ LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME));
            // Indiquer l'arret pour les log
            Runtime.getRuntime().addShutdownHook(new Thread(() -> System.out.println("HDFS stop : "+ LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME) )));
            // Verifier que le dossier data existe sinon le créer
            File data = new File(Project.getDataPath());
            if(!data.exists() && !data.mkdirs()) throw new FileNotFoundException("HIDOOP_HOME/data not found and could not be created.");

            ServerSocket server = new ServerSocket(Constants.PORT);
            while (true) {
                Socket socket = server.accept();
                Slave slave = new Slave(socket);
                slave.start();
            }
        } catch (BindException be){
            be.printStackTrace();
            System.exit(1);
        } catch (Exception e){
            e.printStackTrace();
        }
    } 
}

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
                        if (args.length != 4) {
                            System.err.println("Erreur HDFS_WRITE Serveur : 3 arguments attendus, trouvés "+args.length);
                        } else {
                            File fichier = null;
                            FileOutputStream fileStream = null;
                            try {
                                nameFile = args[1];
                                long minFileSize = Long.parseLong(args[2]);
                                long avgSize = Long.parseLong(args[3]);

                                // Confirmation allocation possible sur le serveur
                                File dir = new File(Project.getDataPath());
                                long usable = dir.getUsableSpace();
                                if (usable <= Constants.CHUNK_LIMIT_FACTOR * avgSize) {
                                    System.err.println("Erreur espace insuffisant : " + usable + " bytes.");
                                    Constants.putLong(buf, Constants.FILE_TOO_LARGE);
                                    ouS.write(buf, 0, Long.BYTES);
                                    socket.close();
                                    return;
                                } else {
                                    Constants.putLong(buf, 10); // Any positive value
                                    ouS.write(buf, 0, Long.BYTES);
                                }

                                int nbytesTotal = 0;
                                /** Buffer */
                                byte[] buffer = new byte[Constants.BUFFER_SIZE];

                                /* Fichier crée */
                                fichier = new File(Project.getDataPath() + nameFile);
                                fileStream = new FileOutputStream(fichier);

                                int end = -1;

                                nbOctetsInLus = inS.read(buffer);
                                // On ne cherche pas la fin du chunk inutilement avant une taille min
                                while ((nbytesTotal += nbOctetsInLus) < minFileSize || (end = Constants.findByte(buffer, Constants.END_CHUNK_DELIMITER, 0, nbOctetsInLus)) == -1) {

                                    if (nbytesTotal > avgSize * Constants.CHUNK_LIMIT_FACTOR) {
                                        // Refuser chunk trop gros (sécurité)
                                        Constants.putLong(buf, Constants.CHUNK_TOO_LARGE);
                                        System.err.println("WR error : " + nbytesTotal + " / "+avgSize);
                                        ouS.write(buf, 0, Long.BYTES);
                                        socket.close();
                                        return;
                                    }
                                    fileStream.write(buffer, 0, nbOctetsInLus);
                                    nbOctetsInLus = inS.read(buffer);
                                }

                                // Mise à jour de la taille écrite
                                nbytesTotal -= (nbOctetsInLus - end);
                                if (nbytesTotal > avgSize * Constants.CHUNK_LIMIT_FACTOR) {
                                    // Refuser chunk trop gros (sécurité)
                                    Constants.putLong(buf, Constants.CHUNK_TOO_LARGE);
                                    System.err.println("WR* error : " + nbytesTotal + " / "+avgSize);
                                    ouS.write(buf, 0, Long.BYTES);
                                    socket.close();
                                    return;
                                } else fileStream.write(buffer, 0, end);


                                if (nbytesTotal > 0) {
                                    Constants.putLong(buf, nbytesTotal);
                                    ouS.write(buf, 0, Long.BYTES);
                                    System.out.println(nameFile + " saved (" + nbytesTotal + " B).");
                                } else {
                                    Constants.putLong(buf, Constants.FILE_EMPTY);
                                    System.err.println(nameFile + " empty.");
                                    ouS.write(buf, 0, Long.BYTES);
                                    fileStream.close();
                                    fichier.delete();
                                }
                            } catch (IOException e) {
                                // Delete part of chunk
                                if(fileStream != null) {
                                    fileStream.close();
                                    fichier.delete();
                                }
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
                if (!socket.isClosed()){
                    try {
                        socket.close();
                    } catch (IOException ex){
                        ex.printStackTrace();
                    }
                }
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

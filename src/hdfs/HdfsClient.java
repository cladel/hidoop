package hdfs;

import config.Project;
import formats.Format;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.concurrent.*;


public class HdfsClient {

    public static final String[] SERVERS_IP = { "192.168.1.57" }; // Liste des ip des HdfsServer
    private static final String DATAFILE_NAME = "meta"; // nom du fichier de métadonnées


    private static void usage() {
        System.out.println("Use: java HdfsClient { -r <file> [localDest] " +
                "| -w <file> -f ln|kv [ --chunks-size=<sizeInBytes>|distributed ] [ --rep=<repFactor> ] " +
                "| -d <file> }\n");
    }


    public static void HdfsList() throws IOException, ClassNotFoundException {
        Metadata data = Metadata.load(new File(Project.PATH+ DATAFILE_NAME));
        System.out.println(data.getFileCount()+ " saved :");
        for (String n : data.getFileNames()){
            System.out.println("    - "+n);
        }
    }


    /**
     * Write a file in HDFS
     * @param fmt format of the file (kv or ln)
     * @param localFSSourceFname local file to add into HDFS
     * @param repFactor number of copy of the same chunk
     * @param chunkSize approached size of the chunks (a chunk may be a little larger/smaller since
     *                  a line is never cut). When <= 0, distributed mode is used, ie if N servers
     *                  are available, the chunks will be split amongst k <= N servers.
     */
    public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, int repFactor, long chunkSize)
            throws IOException, ClassNotFoundException, ExecutionException, InterruptedException {

        Metadata data = Metadata.load(new File(Project.PATH+ DATAFILE_NAME));
        final File local = new File(Project.PATH+localFSSourceFname);
        long size = local.length(); // size in bytes
        int start = 0;

        FileData fd = data.retrieveFileData(localFSSourceFname);
        boolean isNew = (fd == null);

        if (isNew) {
            // Create file
            fd = new FileData(fmt, size, chunkSize);

        } else {
            // TODO append?
            throw new FileAlreadyExistsException(localFSSourceFname);
        }

        // Adapt size to get k <= N chunks
        if (chunkSize <= 0) chunkSize = size / SERVERS_IP.length + (size % SERVERS_IP.length == 0 ? 0 : 1);

        // Count chunks
        int count = (int) (size / chunkSize) + (size % chunkSize == 0 ? 0 : 1);

        System.out.println("Splitting file in "+count+" chunks...");

        // Contact each server and write a chunk using a thread pool
        int procCount = Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(Math.min(count, procCount));
        String chunkName;
        LinkedList<Future<OperationResult<Boolean>>> results = new LinkedList<>();

        int j = 0;
        int id;
        // Submit each chunk to the pool
        for (int i = 0; i < count; i++) {
            id = i + start;
            chunkName = FileData.chunkName(id, localFSSourceFname, fmt);
            Future<OperationResult<Boolean>> b = pool.submit(
                    new Write(chunkName, id, local, chunkSize, (i * chunkSize), SERVERS_IP[j]));
            results.add(b);
            j = (j + 1) % SERVERS_IP.length;
        }

        // Check success and add metadata
        for (Future<OperationResult<Boolean>> b : results) {
            OperationResult<Boolean> res = b.get();
            boolean ok = res.getOk();
            if (ok) fd.addChunkHandle(res.getId(), res.getIpSource());
            else {
                //TODO
                System.err.println("Something went wrong with "+res.getId());
            }

        }
        pool.shutdown();

        // Save updated metadata
        if (isNew) data.addFileData(localFSSourceFname, fd);
        Metadata.save(new File(Project.PATH+ DATAFILE_NAME), data);

        System.out.println(localFSSourceFname + " successfully saved.");

    }


    /**
     * Read a file stored in HDFS
     * @param hdfsFname name of the file
     * @param localFSDestFname local dest file
     */
    public static void HdfsRead(String hdfsFname, String localFSDestFname)
            throws IOException, ClassNotFoundException, ExecutionException, InterruptedException {

        if (localFSDestFname == null) localFSDestFname = "r_"+hdfsFname;

        Metadata data = Metadata.load(new File(Project.PATH+ DATAFILE_NAME));
        FileData fd = data.retrieveFileData(hdfsFname);
        if (fd == null) throw new FileNotFoundException(hdfsFname);

        File local = new File(Project.PATH+localFSDestFname);
        if (local.exists()) throw new FileAlreadyExistsException(localFSDestFname);
        local.createNewFile();

        System.out.println("Reading file...");

        // Contact each server and read chunks using a thread pool
        int procCount = Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(Math.min(fd.getChunkCount(), procCount));
        LinkedList<Future<OperationResult<File>>> results = new LinkedList<>();
        String chunkName;

        // Submit each chunk to the pool
        for (int id : fd.getChunksIds()) {
            String ip = fd.getSourcesForChunk(id).get(0); // Get(0) since rep=1
            chunkName = FileData.chunkName(id, hdfsFname, fd.getFormat());
            Future<OperationResult<File>> b = pool.submit(new Read(chunkName, id, ip, fd.getChunkSize()));
            results.add(b);

        }

        // Append all tmp files in order
        FileOutputStream out = new FileOutputStream(local);
        FileInputStream in;
        for (Future<OperationResult<File>> b : results) {
            OperationResult<File> res = b.get();
            File tmp = res.getOk();
            if (tmp != null) {
                in = new FileInputStream(tmp);
                byte[] a = in.readAllBytes();
                //System.out.println(new String(a, StandardCharsets.UTF_8));
                out.write(a);
                in.close();
                tmp.delete();
            }
            else {
                //TODO
                System.err.println("Something went wrong with "+res.getId());
            }

        }
        pool.shutdown();
        out.close();

        System.out.println(hdfsFname + " successfully read to "+localFSDestFname+".");

    }

    /**
     * Delete file in HDFS
     * @param hdfsFname name of the stored file
     */
    public static void HdfsDelete(String hdfsFname)
            throws IOException, ClassNotFoundException, ExecutionException, InterruptedException {

        Metadata data = Metadata.load(new File(Project.PATH+ DATAFILE_NAME));
        FileData fd = data.retrieveFileData(hdfsFname);
        if (fd == null) throw new FileNotFoundException(hdfsFname);

        System.out.println("Deleting file...");

        int procCount = Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(Math.min(fd.getChunkCount(), procCount));
        LinkedList<Future<OperationResult<Boolean>>> results = new LinkedList<>();
        String chunkName;

        // Submit each chunk to the pool
        for (int id : fd.getChunksIds()) {
            chunkName = FileData.chunkName(id, hdfsFname, fd.getFormat());
            String ip = fd.getSourcesForChunk(id).get(0); // Get(0) since rep=1
            Future<OperationResult<Boolean>> b = pool.submit(new Delete(chunkName, id, ip));
            results.add(b);

        }

        for (Future<OperationResult<Boolean>> b : results) {
            OperationResult<Boolean> res = b.get();
            boolean ok = res.getOk();
            if (!ok) {
                //TODO
                System.err.println("Something went wrong with "+res.getId());
            }

        }
        pool.shutdown();

        // Save updated metadata
        data.removeFileData(hdfsFname);
        Metadata.save(new File(Project.PATH+ DATAFILE_NAME), data);
        System.out.println(hdfsFname + " successfully deleted.");

    }


    /**
     * Result of an operation with HdfsServer
     * @param <T> type of the result information
     */
    private static class OperationResult<T> {
        private final int id;
        private final String ipSource;
        private final T ok;

        private OperationResult(int id, String ipSource, T ok) {
            this.id = id;
            this.ipSource = ipSource;
            this.ok = ok;
        }

        public int getId() {
            return id;
        }

        public String getIpSource() {
            return ipSource;
        }

        public T getOk() {
            return ok;
        }
    }

    /**
     * Callable reading a chunk from an HdfsServer node
     */
    private static class Read implements Callable<OperationResult<File>> {
        private final String command;
        private final File local;
        private final String serverIp;
        private final int id;
        private final long chunkSize;


        public Read(String name, int id, String serverIp, long chunkSize) throws IOException {
            this.command = Commands.HDFS_READ.toString() + " " + name;
            String tmpName = name + ".tmp";
            this.local = new File(Project.PATH+tmpName);
            if (local.exists()) throw new FileAlreadyExistsException(tmpName);
            local.createNewFile();
            this.serverIp = serverIp;
            this.id = id;
            this.chunkSize = chunkSize;
        }


        @Override
        public OperationResult<File> call() {
            try {
                byte[] buf = new byte[Constants.BUFFER_SIZE];
                int read;
                FileOutputStream out = new FileOutputStream(local);
                Socket hdfsSocket = new Socket(serverIp, Constants.PORT);
                OutputStream os = hdfsSocket.getOutputStream();
                InputStream is = hdfsSocket.getInputStream();

                // Send command
                byte[] cmd = command.getBytes(StandardCharsets.US_ASCII);
                cmd = Arrays.copyOf(cmd, Constants.CMD_BUFFER_SIZE);
                os.write(cmd);

                // Copy bytes to file while receiving
                while ((read = is.read(buf)) > 0) {
                    //System.out.print(serverIp+" "+id+" <- "+new String(buf, StandardCharsets.UTF_8));
                    out.write(buf,0,read);
                }

                // CLose connection
                is.close();
                os.close();
                out.close();
                hdfsSocket.close();

                return new OperationResult<>(id, serverIp, local);

            } catch (Exception e) {
                e.printStackTrace();
                return new OperationResult<>(id, serverIp, null);
            }
        }
    }

    /**
     * Callable writing a chunk to an HdfsServer node
     */
    private static class Write implements Callable<OperationResult<Boolean>> {
        private final String command;
        private final File local;
        private final long chunkSize;
        private final long offset;
        private final String serverIp;
        private final int id;

        public Write(String name, int id, File local, long chunkSize, long offset, String serverIp) {

            this.command = Commands.HDFS_WRITE.toString() + " " + name + " " + chunkSize;
            this.local = local;
            this.chunkSize = chunkSize;
            this.offset = offset;
            this.serverIp = serverIp;
            this.id = id;
        }


        @Override
        public OperationResult<Boolean> call() {
            try {
                int sz = (long)Constants.BUFFER_SIZE > chunkSize ? (int) chunkSize : Constants.BUFFER_SIZE;
                byte[] buf = new byte[sz];
                int read = 0, total = 0;

                String line;
                int off, len;

                FileInputStream in = new FileInputStream(local);

                // Read chunksize bytes from source file starting from offset
                in.getChannel().position(offset);
                // Skip the previous line (most likely part of line)
                int ind = 0;

                // Init socket connection
                Socket hdfsSocket = new Socket(serverIp, Constants.PORT);
                OutputStream os = hdfsSocket.getOutputStream();

                // Send command header
                byte[] cmd = command.getBytes(StandardCharsets.UTF_8);
                cmd = Arrays.copyOf(cmd, Constants.CMD_BUFFER_SIZE);
                // System.out.println("Sending to "+serverIp+" : "+new String(cmd, StandardCharsets.UTF_8));
                os.write(cmd);

                /* Send chunk content */

                // If this is not the first chunk, then skip the end of the previous line
                // Else read from 0
                if (offset > 0) {
                    // Read a fist time to skip the previous line and keep reading while no new line is found
                    do {
                        read = in.read(buf);
                        // End of the file reached, no need to create an empty chunk
                        //TODO replace boolean with better info
                        if (read <= 0) {
                            in.close();
                            return new OperationResult<>(id, serverIp, false);
                        }
                        line = new String(buf, StandardCharsets.UTF_8);
                        total += read;

                    } while ((ind = line.indexOf("\n")) == -1);

                    // End of the previous line was found at index ind
                    // Offset placed to next character
                    off = ind+1;
                    len = Math.min(buf.length - ind - 1, read - ind -1);
                        // System.out.println(serverIp+" "+id+" <- skip : '"+line.substring(0,ind)+"'");

                } else {
                    // Read a first time
                    read = in.read(buf);
                    total += read;
                    line = new String(buf, StandardCharsets.UTF_8);
                    len = read;
                    off = 0;

                }

                // Write and read while end of file is not reached and while the chunk or the line is not over
                while (read > 0 && (total <= chunkSize || (ind = line.indexOf("\n")) == -1)) {
                    os.write(buf, off, len);
                    read = in.read(buf);
                    total += read;
                    line = new String(buf, StandardCharsets.UTF_8);
                    len = read;
                    off = 0;
                }
                // Write the end of the line if EOF is not reached
                if (read > 0) os.write(buf, 0, ind+1);

                // Close file and connection
                in.close();
                os.close();
                hdfsSocket.close();


                // Success
                return new OperationResult<>(id, serverIp, true);

            } catch (Exception e) {
                e.printStackTrace();
                // Signal failure
                return new OperationResult<>(id, serverIp, false);
            }
        }
    }



    /**
     * Callable deleting a chunk from an HdfsServer node
     */
    private static class Delete implements Callable<OperationResult<Boolean>> {
        private final String command;
        private final String serverIp;
        private final int id;


        public Delete(String name, int id, String serverIp) {
            this.command = Commands.HDFS_DELETE.toString() + " " + name;
            this.serverIp = serverIp;
            this.id = id;
        }


        @Override
        public OperationResult<Boolean> call() {
            try {

                Socket hdfsSocket = new Socket(serverIp, Constants.PORT);
                OutputStream os = hdfsSocket.getOutputStream();

                // Send command
                byte[] cmd = command.getBytes(StandardCharsets.US_ASCII);
                cmd = Arrays.copyOf(cmd, Constants.CMD_BUFFER_SIZE);
                os.write(cmd);

                // Close connection
                os.close();
                hdfsSocket.close();
                return new OperationResult<>(id, serverIp, true);

            } catch (Exception e) {
                e.printStackTrace();
                return new OperationResult<>(id, serverIp, false);
            }
        }
    }


    public static void main(String[] args) {

        try {
            if (args.length < 2) {
                usage();
                return;
            }
            switch (args[0]) {
                case "-l":
                    HdfsList();
                    break;
                case "-r":
                    HdfsRead(args[1], args.length > 2 ? args[2] : null);
                    break;
                case "-d":
                    HdfsDelete(args[1]);
                    break;
                case "-w":
                    Format.Type fmt;
                    if (args.length > 2 && args[2].equals("-f")) {
                        if (args[3].equals("ln")) fmt = Format.Type.LINE;
                        else if (args[3].equals("kv")) fmt = Format.Type.KV;
                        else {
                            usage();
                            return;
                        }
                        long chunksMode;
                        if (args.length == 5 && args[4].startsWith("--chunks-size=")){
                            String mode = args[4].substring("--chunks-size=".length());
                            if (mode.equals("distributed")) chunksMode = -1;
                            else chunksMode = Long.parseLong(mode);
                        } else {
                            // Default value
                            // TODO this value is for testing
                            chunksMode = 150;
                        }
                        HdfsWrite(fmt, args[1], 1, chunksMode);
                    } else {
                        usage();
                    }

            }
        } catch (FileNotFoundException | FileAlreadyExistsException ferr){
            System.err.println(ferr.getClass().getSimpleName()+" : "+ferr.getMessage());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}

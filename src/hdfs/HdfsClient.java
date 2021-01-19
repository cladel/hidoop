package hdfs;

import config.FileData;
import config.AppData;
import config.Metadata;
import config.Project;
import formats.Format;
import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.*;


public class HdfsClient {
    private static AppData data;
    private static final boolean verbose = false; // Useful for DEBUG


    private static void usage() {
        System.out.println("Use: java HdfsClient { -r <file> [localDest] " +
                "| -w <file> [-f ln|kv] [ --chunks-size=<sizeInBytes> ] [ --rep=<repFactor> ] " +
                "| -d <file> " +
                "| -l }\n"+
                "Default format is ln. \n" +
                "--rep is currently not supported and is always 1.");
    }

    /**
     * Get path for file.
     * If the given file name starts with '/' it is considered as an absolute path
     * and will be used as such. Otherwise, the relative path in $HIDOOP_HOME will
     * be used.
     * @param file If empty, returns $HIDOOP_HOME/data.
     * @return path for this file
     */
    private static String getPathForFile(String file){
        if (file.trim().charAt(0)=='/') return file; // Chemin absolu
        else return Project.getDataPath()+file; // Chemin relatif à HIDOOP_HOME
    }


    /**
     * Print file list
     */
    public static void HdfsList() {
        DateFormat df = new SimpleDateFormat();
        Metadata data = HdfsClient.data.getMetadata();
        FileData fd;
        String size;

        System.out.println(data.getFileCount()+ " saved (" + df.format(data.getSaveDate())+") :");

        for (String n : data.getFileNames()){
            fd = data.retrieveFileData(n);
            size = fd.getFileSize() >= 0 ? fd.getFileSize()+" B" : "UNKNOWN SIZE";
            System.out.println("    - " + n + " (" + size + ")");

        }
        System.out.println("-----------------------------");
    }


    /**
     * Write a file in HDFS
     * @param fmt format of the file (KEY or LINE)
     * @param localFSSourceFname local file to add into HDFS
     * @param repFactor number of copies of the same chunk
     * @param chunkSize approached size of the chunks (a chunk may be a little larger/smaller since
     *                  a line is never cut). When <= 0, distributed mode is used, ie if N servers
     *                  are available, the chunks will be split amongst k <= N servers.
     */
    public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, int repFactor, long chunkSize)
            throws IOException, ExecutionException, InterruptedException {

        // Control fileName length because of buffers
        if (localFSSourceFname.length() > Constants.MAX_NAME_LENGTH) localFSSourceFname = localFSSourceFname.substring(0,Constants.MAX_NAME_LENGTH);

        // Get metadata and servers location
        Metadata data = HdfsClient.data.getMetadata();
        final String[] SERVERS_IP = HdfsClient.data.getServersIp();
        if (SERVERS_IP.length == 0) throw new UnsupportedOperationException("No server found.");

        final File local = new File(getPathForFile(localFSSourceFname));
        localFSSourceFname = local.getName();

        long size = local.length(); // size in bytes
        int start = 0;

        FileData fd = data.retrieveFileData(localFSSourceFname);
        boolean isNew = (fd == null);

        if (isNew) {
            // Create file
            fd = new FileData(fmt, size, chunkSize);

        } else {
            // TODO is append possible?
            throw new FileAlreadyExistsException(localFSSourceFname);
        }

        // Use default size
        if (chunkSize <= 0) chunkSize = HdfsClient.data.getDefaultChunkSize();   // distributed : chunkSize = size / SERVERS_IP.length + (size % SERVERS_IP.length == 0 ? 0 : 1);


        // Count chunks
        int count = (int) (size / chunkSize) + (size % chunkSize == 0 ? 0 : 1);

        System.out.println("Splitting file in "+count+" chunks...");

        // Contact each server and write a chunk using a thread pool
        int procCount = Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(Math.min(count, procCount));
        String chunkName;
        LinkedList<Future<OperationResult<Long>>> results = new LinkedList<>();

        int j = 0;
        int id;
        // Submit each chunk to the pool
        for (int i = 0; i < count; i++) {
            id = i + start;
            chunkName = FileData.chunkName(id, localFSSourceFname, fmt);
            Future<OperationResult<Long>> b = pool.submit(
                    new Write(chunkName, id, local, chunkSize, (i * chunkSize), SERVERS_IP[j]));
            results.add(b);
            j = (j + 1) % SERVERS_IP.length;
        }

        // Check success and add metadata
        boolean allOk = true;
        for (Future<OperationResult<Long>> b : results) {
            OperationResult<Long> res = b.get();
            long resCode = res.getRes();
            if (resCode == 0){
                fd.addChunkHandle(res.getId(), res.getIpSource());

            } else if (resCode != Constants.FILE_EMPTY) { // Just ignore an empty chunk
                allOk = false;
                // Print error code
                System.err.println(res.getIpSource()+ " : (chunkID "+res.getId()+") : error "+res.getRes());
            }

        }
        pool.shutdown();

        // Save updated metadata
        if (allOk) {
            //if (isNew)
            data.addFileData(localFSSourceFname, fd);
            HdfsClient.data.saveMetadata(data);
            System.out.println(localFSSourceFname + " successfully saved.");
        }


    }


    /**
     * Read a file stored in HDFS
     * @param hdfsFname name of the file
     * @param localFSDestFname local dest file
     */
    public static void HdfsRead(String hdfsFname, String localFSDestFname)
            throws IOException, ExecutionException, InterruptedException {

        if (localFSDestFname == null) localFSDestFname = "r_"+hdfsFname;

        Metadata data = HdfsClient.data.getMetadata();
        FileData fd = data.retrieveFileData(hdfsFname);
        if (fd == null) throw new FileNotFoundException(hdfsFname);

        File local = new File(getPathForFile(localFSDestFname));
        localFSDestFname = local.getName();
        if (local.exists()) throw new FileAlreadyExistsException(localFSDestFname);
        local.createNewFile();

        System.out.println("Reading file...");

        // Contact each server and read chunks using a thread pool
        int procCount = Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(Math.min(fd.getChunkCount(), procCount));
        LinkedList<Future<OperationResult<Map.Entry<Long,File>>>> results = new LinkedList<>();
        String chunkName;

        // Submit each chunk to the pool
        for (int id : fd.getChunksIds()) {
            String ip = fd.getSourcesForChunk(id).get(0); // Get(0) since rep=1
            chunkName = FileData.chunkName(id, hdfsFname, fd.getFormat());
            Future<OperationResult<Map.Entry<Long,File>>> b = pool.submit(new Read(chunkName, id, ip));
            results.add(b);

        }

        // Append all tmp files in order
        FileOutputStream out = new FileOutputStream(local);
        FileInputStream in;
        byte[] buf = new byte[Constants.BUFFER_SIZE];
        int read;
        boolean allOk = true;

        for (Future<OperationResult<Map.Entry<Long,File>>> b : results) {
            OperationResult<Map.Entry<Long,File>> res = b.get();
            Map.Entry<Long,File> resObj = res.getRes();
            File tmp = resObj.getValue();
            long resCode = resObj.getKey();

            // Append only if there's no error
            if (resCode == 0 && allOk) {
                if (tmp != null) {
                    in = new FileInputStream(tmp);
                    while ((read = in.read(buf)) > 0) {
                        out.write(buf, 0, read);
                    }

                    in.close();
                    tmp.delete();
                }
            }
            else {
                allOk = false;
                if (tmp != null) tmp.delete();
                System.err.println(res.getIpSource()+ " : (chunkID "+res.getId()+") : error "+resCode);
            }

        }
        pool.shutdown();
        out.close();
        if (allOk) System.out.println(hdfsFname + " successfully read to "+localFSDestFname+".");
        else local.delete();

    }

    /**
     * Delete file in HDFS
     * @param hdfsFname name of the stored file
     */
    public static void HdfsDelete(String hdfsFname)
            throws IOException, ExecutionException, InterruptedException {

        Metadata data = HdfsClient.data.getMetadata();
        FileData fd = data.retrieveFileData(hdfsFname);
        if (fd == null) throw new FileNotFoundException(hdfsFname);

        System.out.println("Deleting file...");

        int procCount = Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(Math.min(fd.getChunkCount(), procCount));
        LinkedList<Future<OperationResult<Long>>> results = new LinkedList<>();
        String chunkName;

        // Submit each chunk to the pool
        for (int id : fd.getChunksIds()) {
            chunkName = FileData.chunkName(id, hdfsFname, fd.getFormat());
            String ip = fd.getSourcesForChunk(id).get(0); // Get(0) since rep=1
            Future<OperationResult<Long>> b = pool.submit(new Delete(chunkName, id, ip));
            results.add(b);

        }
        boolean allOk = true;
        for (Future<OperationResult<Long>> b : results) {
            OperationResult<Long> res = b.get();
            boolean ok = (res.getRes() == 0);
            if (!ok) {
                allOk = false;
                // Print error code
                System.err.println(res.getIpSource()+ " : (chunkID "+res.getId()+") : error "+res.getRes());
            }

        }
        pool.shutdown();

        // Save updated metadata
        if (allOk) {
            data.removeFileData(hdfsFname);
            HdfsClient.data.saveMetadata(data);
            System.out.println(hdfsFname + " successfully deleted.");
        }

    }


    /**
     * Result of an operation with HdfsServer
     * @param <T> type of the result information
     */
    private static class OperationResult<T> {
        private final int id;
        private final String ipSource;
        private final T res;

        private OperationResult(int id, String ipSource, T res) {
            this.id = id;
            this.ipSource = ipSource;
            this.res = res;
        }

        public int getId() {
            return id;
        }

        public String getIpSource() {
            return ipSource;
        }

        public T getRes() {
            return res;
        }
    }

    /**
     * Callable reading a chunk from an HdfsServer node
     */
    private static class Read implements Callable<OperationResult<Map.Entry<Long, File>>> {
        private final String command;
        private final File local;
        private final String serverIp;
        private final int id;


        public Read(String name, int id, String serverIp) throws IOException {
            this.command = Commands.HDFS_READ.toString() + " " + name;
            String tmpName = name + ".tmp";
            this.local = new File(Project.getDataPath()+(tmpName));
            local.createNewFile();
            this.serverIp = serverIp;
            this.id = id;
        }


        @Override
        public OperationResult<Map.Entry<Long, File>> call() {

            try {
                Socket hdfsSocket = new Socket(serverIp, Constants.PORT);
                OutputStream os = hdfsSocket.getOutputStream();
                InputStream is = hdfsSocket.getInputStream();

                // Send command
                byte[] cmd = command.getBytes(StandardCharsets.US_ASCII);
                cmd = Arrays.copyOf(cmd, Constants.CMD_BUFFER_SIZE);
                os.write(cmd);

                // Get chunk size info from the server
                is.readNBytes(cmd, 0, Long.BYTES);
                long length = Constants.getLong(cmd);


                if (length <= 0) {
                    hdfsSocket.close();
                    return new OperationResult<>(id, serverIp, Map.entry((long)Constants.FILE_NOT_FOUND, local));
                }

                FileOutputStream out = new FileOutputStream(local);
                long total = 0;
                int read;
                byte[] buf = new byte[Constants.BUFFER_SIZE];

                // Copy bytes to file while receiving expected bytes
                while (total < length && (read = is.read(buf)) > 0) {
                    //System.out.print(serverIp+" "+id+" <- "+new String(buf, StandardCharsets.UTF_8));
                    out.write(buf,0,read);
                    total += read;
                }

                long status = 0;
                // Check size integrity
                if (total != length) {
                    status = Constants.INCONSISTENT_FILE_SIZE;
                    System.err.println("RD error : "+total+" "+length);
                }


                // CLose connection
                out.close();
                hdfsSocket.close();

                return new OperationResult<>(id, serverIp, Map.entry(status, local));

            } catch (Exception e) {
                e.printStackTrace();
                return new OperationResult<>(id, serverIp, Map.entry(Constants.IO_ERROR, local));
            }
        }
    }

    /**
     * Callable writing a chunk to an HdfsServer node
     */
    private static class Write implements Callable<OperationResult<Long>> {
        private final String command;
        private final File local;
        private final long chunkSize;
        private final long offset;
        private final String serverIp;
        private final int id;

        public Write(String name, int id, File local, long chunkSize, long offset, String serverIp) {

            this.command = Commands.HDFS_WRITE.toString() + " " + name + " ";
            this.local = local;
            this.chunkSize = chunkSize;
            this.offset = offset;
            this.serverIp = serverIp;
            this.id = id;
        }


        @Override
        public OperationResult<Long> call() {
            try {

                // Size of the io and tcp buffer
                int size = (long)Constants.BUFFER_SIZE > chunkSize ? (int) chunkSize : Constants.BUFFER_SIZE;
                byte[] buf = new byte[size];
                // Total bytes read in the file; length sent to the server; beginning of this chunk is at offset + prevLineOffset
                long total = 0, totalWritten = 0, prevLineOffset = 0;

                int read, off, len, ind = 0;

                // Read chunksize bytes from source file starting from offset
                FileInputStream in = new FileInputStream(local);
                in.getChannel().position(offset);


                // If this is not the first chunk, then skip the end of the previous line
                // Else read from 0
                if (offset > 0) {
                    // Read a fist time to skip the previous line and keep reading while no new line is found
                    do {
                        read = in.read(buf);
                        // End of the file reached, no need to create an empty chunk
                        if (read <= 0) {
                            in.close();
                            return new OperationResult<>(id, serverIp, Constants.FILE_EMPTY);
                        }
                        total += read;

                    } while ((ind = Constants.findByte(buf, Constants.NEW_LINE, 0, buf.length)) == -1);


                    // End of the previous line was found at index ind
                    // Offset placed to next character
                    off = ind+1;
                    len = read - ind - 1;
                    //System.out.println(serverIp+" "+id+" <- skip : '"+line.substring(0,ind+1)+"'");
                    prevLineOffset = total - len;


                } else {
                    // Read a first time
                    read = in.read(buf);
                    total += read;
                    len = read;
                    off = 0;
                }

                // Init socket connection
                Socket hdfsSocket = new Socket(serverIp, Constants.PORT);
                OutputStream os = hdfsSocket.getOutputStream();
                InputStream is = hdfsSocket.getInputStream();



                // Minimum nb of bytes sent is chunkSize or remaining size of file minus the ignored end of previous line
                long minChunkSize = Math.min(chunkSize, local.length() - offset) - prevLineOffset;

                // Send command header
                byte[] cmd;
                cmd = command.concat(String.valueOf(minChunkSize)).getBytes(StandardCharsets.UTF_8);
                cmd = Arrays.copyOf(cmd, Constants.CMD_BUFFER_SIZE);
                //System.out.println("Sending to "+serverIp+" : "+new String(cmd, StandardCharsets.UTF_8));
                os.write(cmd);

                // Wait for server available space confirmation
                if (verbose) System.out.println(id + " awaiting server response...");
                is.readNBytes(cmd, 0, Long.BYTES);
                long res = Constants.getLong(cmd);
                if (res != 0){
                    // If error stop exchange
                    in.close();
                    hdfsSocket.close();
                    return new OperationResult<>(id, serverIp, res);
                }


                /* Send chunk content */

                // Write and read while end of file is not reached and while the chunk or the line is not over
                // If the total read is bg or eq than the chunkSize, start looking for '\n' at the index where chunkSize
                // bytes were read in total
                while (read > 0 && (total <= chunkSize ||
                        (ind = Constants.findByte(buf, Constants.NEW_LINE, (int)(read - (total - chunkSize)), buf.length)) == -1))
                {
                    totalWritten += len;
                    os.write(buf, off, len);
                    read = in.read(buf);
                    total += read;
                    len = read;
                    off = 0;
                }



                // Write the end of the line if EOF was not reached and send end of chunk delimiter
                if (read > 0){
                    totalWritten += ind+1;
                    buf[ind+1] = Constants.END_CHUNK_DELIMITER;
                    os.write(buf, 0, ind+2);
                } else {
                    os.write(Constants.END_CHUNK_DELIMITER);
                }

                long status = 0;

                if (verbose) System.out.println(id + " awaiting server validation...");
                is.readNBytes(cmd, 0,  Long.BYTES);
                long written = Constants.getLong(cmd);

                if (written != totalWritten){
                    status = Constants.INCONSISTENT_FILE_SIZE;
                    if (verbose) System.out.println(id+" WR error : "+written+" / "+totalWritten);
                }


                // Close file and connection
                in.close();
                hdfsSocket.close();


                // Success
                return new OperationResult<>(id, serverIp, status);

            } catch (Exception e) {
                e.printStackTrace();
                // Signal failure
                return new OperationResult<>(id, serverIp, Constants.IO_ERROR);
            }
        }
    }



    /**
     * Callable deleting a chunk from an HdfsServer node
     */
    private static class Delete implements Callable<OperationResult<Long>> {
        private final String command;
        private final String serverIp;
        private final int id;


        public Delete(String name, int id, String serverIp) {
            this.command = Commands.HDFS_DELETE.toString() + " " + name;
            this.serverIp = serverIp;
            this.id = id;
        }


        @Override
        public OperationResult<Long> call() {
            try {

                Socket hdfsSocket = new Socket(serverIp, Constants.PORT);
                OutputStream os = hdfsSocket.getOutputStream();
                InputStream is = hdfsSocket.getInputStream();

                // Send command
                byte[] cmd = command.getBytes(StandardCharsets.US_ASCII);
                cmd = Arrays.copyOf(cmd, Constants.CMD_BUFFER_SIZE);
                os.write(cmd);

                // Check status
                is.readNBytes(cmd, 0, Long.BYTES);
                long status = Constants.getLong(cmd);

                // Close connection
                hdfsSocket.close();
                return new OperationResult<>(id, serverIp, status);

            } catch (Exception e) {
                e.printStackTrace();
                return new OperationResult<>(id, serverIp, Constants.IO_ERROR);
            }
        }
    }

    /**
     * Use given configuration.
     * Calling any method from code without setting this first will result in
     * a NullPointerException.
     * @param data config and metadata
     */
    public static void useData(AppData data) {
        HdfsClient.data = data;
    }

    public static void main(String[] args) {

        try {
            if (args.length < 1) {
                usage();
                return;
            }
            long start;

            switch (args[0]) {
                case "-l":
                    data = AppData.loadConfigAndMeta(false);
                    start = System.currentTimeMillis();
                    HdfsList();
                    if (verbose) System.out.println("Durée d'exécution (ms) : "+(System.currentTimeMillis() - start));
                    break;
                case "-r":
                    data = AppData.loadConfigAndMeta(false);
                    start = System.currentTimeMillis();
                    HdfsRead(args[1], args.length > 2 ? args[2] : null);
                    if (verbose) System.out.println("Durée d'exécution (ms) : "+(System.currentTimeMillis() - start));
                    break;
                case "-d":
                    data = AppData.loadConfigAndMeta(false);
                    start = System.currentTimeMillis();
                    HdfsDelete(args[1]);
                    if (verbose) System.out.println("Durée d'exécution (ms) : "+(System.currentTimeMillis() - start));
                    break;
                case "-w":
                    Format.Type fmt = Format.Type.LINE;
                    long chunksMode = -1;
                    int next = 2;
                    int rep = 1;
                    while (args.length > next){

                        if (args[next].equals("-f")) {
                            boolean correct_size = args.length > next + 1;
                            if (correct_size && args[next + 1].equals("ln")) fmt = Format.Type.LINE;
                            else if (correct_size && args[next + 1].equals("kv")) fmt = Format.Type.KV;
                            else {
                                usage();
                                return;
                            }
                            next += 2;
                        } else if (args[next].startsWith("--chunks-size=")) {
                            String mode = args[next].substring("--chunks-size=".length());
                            chunksMode = Long.parseLong(mode);
                            next++;
                        } else if (args[next].startsWith("--rep=")){
                            /* pas utile pour cette version
                            String r = args[next].substring("--rep=".length());
                            if(r.matches("[0-9]+")) rep = Integer.parseInt(r);
                            else {
                                usage();
                                return;
                            }
                            next++;
                            */
                            usage();
                            return;
                        } else {
                            usage();
                            return;
                        }

                    }
                    // Ignoring rep for now
                    data = AppData.loadConfigAndMeta(true);
                    start = System.currentTimeMillis();
                    HdfsWrite(fmt, args[1], rep, chunksMode);
                    if (verbose) System.out.println("Durée d'exécution (ms) : "+(System.currentTimeMillis() - start));
                    break;
                default: usage();
            }
        } catch (FileNotFoundException | FileAlreadyExistsException ferr){
            System.err.println(ferr.getClass().getSimpleName()+" : "+ferr.getMessage());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}

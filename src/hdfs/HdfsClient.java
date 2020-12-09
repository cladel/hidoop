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

    public static final String[] ips = { "192.168.1.22", "192.168.1.57"}; // TODO ask if ip known or to be looked for
    private static final String DATAFILE_NAME = "meta";
    public static final int PORT = 3000;
    public static final int BUFFER_SIZE = 1024;


    private static void usage() {
        System.out.println("Use: java HdfsClient [-r <file> [localDest] | -w <file> -f ln|kv | -d <file>]\n" +
                "Currently supported formats are .kv (Key-Value) and .ln (Line).");
    }

    public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, int repFactor)
            throws IOException, ClassNotFoundException, ExecutionException, InterruptedException {

        Metadata data = Metadata.load(new File(Project.PATH+ DATAFILE_NAME));
        final File local = new File(Project.PATH+localFSSourceFname);
        long size = local.length();
        int chunkSize;
        int start = 0;

        FileData fd = data.retrieveFileData(localFSSourceFname);
        boolean isNew = (fd == null);

        if (isNew) {
            // Create file
            chunkSize = (int) (size / ips.length);
            fd = new FileData(fmt, size, chunkSize);

        } else {
            // Append to file
            // TODO à implementer ?
            throw new FileAlreadyExistsException(localFSSourceFname);
           // chunkSize = fd.getChunkSize();
           // start = fd.getChunkCount();

        }

        int count = (int) (size / chunkSize);

        // Contact each server and write a chunk using a thread pool
        int procCount = Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(Math.min(count, procCount));
        String chunkName = nameToChunkName(localFSSourceFname, fmt);
        LinkedList<Future<OperationResult<Boolean>>> results = new LinkedList<>();

        int j = 0;
        int id;
        for (int i = 0; i < count; i++) {
            id = i + start;
            Future<OperationResult<Boolean>> b = pool.submit(new Write(chunkName, id, local, chunkSize, (i * chunkSize), ips[j]));
            results.add(b);
            j = (j + 1) % ips.length;
        }

        // Check success and add metadata
        for (Future<OperationResult<Boolean>> b : results) {
            OperationResult<Boolean> res = b.get();
            boolean ok = res.getOk();
            if (ok) fd.addChunkHandle(res.getId(), res.getIpSource());
            else {
            } //TODO treat pb

        }
        pool.shutdown();
        if (isNew) data.addFileData(localFSSourceFname, fd);

    }


    public static void HdfsRead(String hdfsFname, String localFSDestFname)
            throws IOException, ClassNotFoundException, ExecutionException, InterruptedException {

        Metadata data = Metadata.load(new File(Project.PATH+ DATAFILE_NAME));
        FileData fd = data.retrieveFileData(hdfsFname);
        if (fd == null) throw new FileNotFoundException();

        File local = new File(Project.PATH+localFSDestFname);
        if (local.exists()) throw new FileAlreadyExistsException(localFSDestFname);
        local.createNewFile();

        // Contact each server and read chunks using a thread pool
        int procCount = Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(Math.min(fd.getChunkCount(), procCount));
        LinkedList<Future<OperationResult<File>>> results = new LinkedList<>();
        String chunkName = nameToChunkName(hdfsFname, fd.getFormat());

        for (int id : fd.getChunksIds()) {
            String ip = fd.getSourcesForChunk(id).get(0); // Get(0) since rep=1
            Future<OperationResult<File>> b = pool.submit(new Read(chunkName, id, ip, fd.getChunkSize()));
            results.add(b);

        }

        FileOutputStream out = new FileOutputStream(local);
        FileInputStream in;
        for (Future<OperationResult<File>> b : results) {
            OperationResult<File> res = b.get();
            File desc = res.getOk();
            if (desc != null) {
                in = new FileInputStream(local);
                out.write(in.readAllBytes());
                in.close();
                desc.delete();
            }
            else {}//TODO treat pb

        }
        pool.shutdown();
        out.close();

    }

    public static void HdfsDelete(String hdfsFname)
            throws IOException, ClassNotFoundException, ExecutionException, InterruptedException {

        Metadata data = Metadata.load(new File(Project.PATH+ DATAFILE_NAME));
        FileData fd = data.retrieveFileData(hdfsFname);
        if (fd == null) throw new FileNotFoundException();

        int procCount = Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(Math.min(fd.getChunkCount(), procCount));
        LinkedList<Future<OperationResult<Boolean>>> results = new LinkedList<>();
        String chunkName = nameToChunkName(hdfsFname, fd.getFormat());

        for (int id : fd.getChunksIds()) {
            String ip = fd.getSourcesForChunk(id).get(0); // Get(0) since rep=1
            Future<OperationResult<Boolean>> b = pool.submit(new Delete(chunkName, id, ip));
            results.add(b);

        }

        for (Future<OperationResult<Boolean>> b : results) {
            OperationResult<Boolean> res = b.get();
            boolean ok = res.getOk();
            if (!ok) { } //TODO treat pb

        }
        pool.shutdown();
        data.removeFileData(hdfsFname);


    }

    private static String nameToChunkName(String fileName, Format.Type fmt){
        return fileName + (fmt == Format.Type.KV ? ".kv" : ".ln");
    }


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

    private static class Read implements Callable<OperationResult<File>> {
        private final String command;
        private final File local;
        private final String serverIp;
        private final int id;
        private final int chunkSize;


        public Read(String name, int id, String serverIp, int chunkSize) throws IOException {
            this.command = Commands.HDFS_READ.toString() + " " + name;
            String tmpName = name + "_" + id + ".tmp";
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
                byte[] buf = new byte[BUFFER_SIZE];
                int read, total = 0;
                FileOutputStream out = new FileOutputStream(local);
                Socket hdfsSocket = new Socket(serverIp, PORT);
                OutputStream os = hdfsSocket.getOutputStream();
                InputStream is = hdfsSocket.getInputStream();

                byte[] cmd = command.getBytes(StandardCharsets.US_ASCII);
                os.write(cmd);

                while (total < chunkSize && (read = is.read(buf)) > 0) {
                    total += read;
                    out.write(buf,0,read);
                }

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

    private static class Write implements Callable<OperationResult<Boolean>> {
        private final String command;
        private final File local;
        private final int chunkSize;
        private final int offset;
        private final String serverIp;
        private final int id;

        public Write(String name, int id, File local, int chunkSize, int offset, String serverIp) {

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
                byte[] buf = new byte[BUFFER_SIZE];
                int read, total = 0;
                FileInputStream in = new FileInputStream(local);
                Socket hdfsSocket = new Socket(serverIp, PORT);
                OutputStream os = hdfsSocket.getOutputStream();

                byte[] cmd = command.getBytes(StandardCharsets.US_ASCII);
                cmd = Arrays.copyOf(cmd, BUFFER_SIZE);
                
                System.out.println("Sending to "+serverIp+" : "+new String(cmd, StandardCharsets.US_ASCII));

                os.write(cmd);

                in.getChannel().position(offset);

                while (total < chunkSize && (read = in.read(buf)) > 0) {
                    total += read;
                    os.write(buf,0,read);
                }

                in.close();
                os.close();

                hdfsSocket.close();
                return new OperationResult<>(id, serverIp, true);

            } catch (Exception e) {
                e.printStackTrace();
                return new OperationResult<>(id, serverIp, false);
            }
        }
    }

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

                Socket hdfsSocket = new Socket(serverIp, PORT);
                OutputStream os = hdfsSocket.getOutputStream();

                byte[] cmd = command.getBytes(StandardCharsets.US_ASCII);
                os.write(cmd);

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
                case "-r":
                    HdfsRead(args[1], args.length > 2 ? args[2] : null);
                    break;
                case "-d":
                    HdfsDelete(args[1]);
                    break;
                case "-w":
                    Format.Type fmt;
                    String fileName = args[1];
                    if (args.length > 2 && args[2].equals("-f")) {
                        if (args[3].equals("ln")) fmt = Format.Type.LINE;
                        else if (args[3].equals("kv")) fmt = Format.Type.KV;
                        else {
                            usage();
                            return;
                        }
                        HdfsWrite(fmt, args[1], 1);
                    } else {
                        usage();
                    }

            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}

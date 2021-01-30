package hdfs;

import config.FileData;
import config.Project;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * class representing an HDFS client/server reading task.
 */
public class Reader extends ClientServerTask<Map.Entry<Long, File>> {

    private final FileData fd;
    private final String hdfsFname;
    private final byte[] buf = new byte[Constants.BUFFER_SIZE];
    private final FileOutputStream out;

    public Reader(FileData fd, String hdfsFname, File local) throws FileNotFoundException {
        super(fd.getChunkCount(), true);
        this.fd = fd;
        this.hdfsFname = hdfsFname;
        out = new FileOutputStream(local, false);
    }

    @Override
    public boolean exec() {
        boolean ok = super.exec();
        try {
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ok;
    }

    @Override
    Callable<OperationResult<Map.Entry<Long, File>>> submitTask(int i) {
        int id = fd.getChunksIds().get(i);
        String ip = fd.getSourcesForChunk(id).get(0); // Get(0) since rep=1
        String chunkName = FileData.chunkName(id, hdfsFname, fd.getFormat());
        return new Read(chunkName, id, ip);
    }

    @Override
    boolean onResult(OperationResult<Map.Entry<Long, File>> res) {
        // Append all tmp files in order

        Map.Entry<Long, File> resObj = res.getRes();
        File tmp = resObj.getValue();
        long resCode = resObj.getKey();

        // Append only if there's no error
        if (resCode == 0) {
            if (tmp == null) return false;
            else {
                try {
                    FileInputStream in = new FileInputStream(tmp);
                    int read;
                    while ((read = in.read(buf)) > 0) {
                        out.write(buf, 0, read);
                    }
                    in.close();

                } catch (IOException e) {
                    e.printStackTrace();
                    return false;
                } finally {
                    tmp.deleteOnExit();
                }
                return true;
            }

        } else {
            if (tmp != null) tmp.deleteOnExit();
            System.err.println(res.getIpSource() + " : (chunkID " + res.getId() + ") error " + resCode);
            return false;
        }


    }

    /**
     * Callable reading a chunk from an HdfsServer node
     */
    protected static class Read implements Callable<OperationResult<Map.Entry<Long, File>>> {
        private final String command;
        private final File local;
        private final String serverIp;
        private final int id;


        public Read(String name, int id, String serverIp) {
            File local1;
            this.command = Commands.HDFS_READ.toString() + " " + name;
            String tmpName = name + ".tmp";
            this.serverIp = serverIp;
            this.id = id;
            try {
                local1 = new File(Project.getDataPath() + (tmpName));
                local1.createNewFile();
            } catch (IOException e) {
                local1 = null;
                e.printStackTrace();
            }
            this.local = local1;
        }


        @Override
        public OperationResult<Map.Entry<Long, File>> call() {

            try {
                if (local == null) throw new IOException("Error creating tmp file.");
                Socket hdfsSocket = new Socket(serverIp, Constants.PORT);
                hdfsSocket.setSoTimeout(120*1000); // Wait read for max 120 seconds
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
                    return new OperationResult<>(id, serverIp, Map.entry((long) Constants.FILE_NOT_FOUND, local));
                }

                FileOutputStream out = new FileOutputStream(local);
                long total = 0;
                int read;
                byte[] buf = new byte[Constants.BUFFER_SIZE];

                // Copy bytes to file while receiving expected bytes
                while (total < length && (read = is.read(buf)) > 0) {
                    //System.out.print(serverIp+" "+id+" <- "+new String(buf, StandardCharsets.UTF_8));
                    out.write(buf, 0, read);
                    total += read;
                }

                long status = 0;
                // Check size integrity
                if (total != length) {
                    status = Constants.INCONSISTENT_FILE_SIZE;
                    System.err.println(serverIp + " (" + id + ") RD error : " + total + " " + length);
                }


                // CLose connection
                out.close();
                hdfsSocket.close();

                return new OperationResult<>(id, serverIp, Map.entry(status, local));

            } catch (Exception e) {
                System.out.println(serverIp + " (" + id + ") : " + e.getMessage());
                e.printStackTrace();
                return new OperationResult<>(id, serverIp, Map.entry(Constants.IO_ERROR, local));
            }
        }
    }

}

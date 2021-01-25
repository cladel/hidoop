package hdfs;

import config.FileData;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;


/**
 * class representing an HDFS client/server deleting task.
 */
public class Deleter extends ClientServerTask<Long> {

    private final FileData fd;
    private final String hdfsFname;


    public Deleter(FileData fd, String hdfsFname) {
        super(fd.getChunkCount(), false);
        this.fd = fd;
        this.hdfsFname = hdfsFname;
    }


    @Override
    Callable<OperationResult<Long>> submitTask(int i) {
        int id = fd.getChunksIds().get(i);
        String chunkName = FileData.chunkName(id, hdfsFname, fd.getFormat());
        String ip = fd.getSourcesForChunk(id).get(0); // Get(0) since rep=1
        return new Delete(chunkName, id, ip);
    }

    @Override
    boolean onResult(OperationResult<Long> res) {
        boolean ok = (res.getRes() == 0);
        if (!ok) {
            // Print error code
            System.err.println(res.getIpSource()+ " : (chunkID "+res.getId()+") error "+res.getRes());
        }
        return ok;
    }

    @Override
    void onAbort(List<Future<OperationResult<Long>>> results) {
        // Keep sending delete cmd here
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
                System.out.println(serverIp+" ("+id+") : "+e.getMessage());
                e.printStackTrace();
                return new OperationResult<>(id, serverIp, Constants.IO_ERROR);
            }
        }
    }

}

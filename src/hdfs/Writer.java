package hdfs;

import config.FileData;
import formats.Format;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.*;

/**
 * class representing an HDFS client/server writing task.
 */
public class Writer extends ClientServerTask<Long> {

    private final long chunkSize;
    private final String[] SERVERS_IP;
    private final File local;
    private final Format.Type fmt;
    private final int start_id;
    private final String localFSSourceFname;
    private final FileData fd;
    private int serverIndex = 0;
    private int progress = 0;
    private int chunkCount = count;

    protected Writer(FileData fd, String[] servers, File local, long chunkSize, Format.Type fmt) {
        super((int) (local.length() / chunkSize) + (local.length() % chunkSize == 0 ? 0 : 1), true);
        this.fd = fd;
        this.fmt = fmt;
        this.SERVERS_IP = servers;
        this.local = local;
        this.chunkSize = chunkSize;
        this.localFSSourceFname = local.getName();
        this.start_id = 0;
    }


    public boolean exec() {
        System.out.println("Splitting file in "+count+" chunks...");
        return super.exec();
    }



    @Override
    Write submitTask(int i) {
        int id = i + start_id;
        String chunkName = FileData.chunkName(id, localFSSourceFname, fmt);
        serverIndex = (serverIndex + 1) % SERVERS_IP.length;
        return new Write(chunkName, id, local, chunkSize, ((long) i * chunkSize), SERVERS_IP[serverIndex]);

    }

    @Override
    public int getProgress() {
        return (progress * 100 / chunkCount);
    }

    @Override
    boolean onResult(OperationResult<Long> res) {

        long resCode = res.getRes();
        if (resCode == 0) {
            fd.addChunkHandle(res.getId(), res.getIpSource());
            progress++;
            return true;
        } else if (resCode == Constants.FILE_EMPTY) { // Just ignore an empty chunk
            chunkCount--;
            return true;
        } else {

            // Print error code
            System.err.println(res.getIpSource() + " : (chunkID " + res.getId() + ") error " + res.getRes());
            if (resCode == Constants.CHUNK_TOO_LARGE) {
                // Chunk too large due to line too long
                // Chunksize is not adapted to this file
                System.out.println("\nThere probably is a line longer than twice a chunk size. Aborting...");

                // Abort and cancel previous chunks write
                // Reference file to delete previously written chunks

                return false;

            } else {
                // TODO implement retry mechanism
                System.out.println("\nError. Aborting...");
                return false;
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

            this.command = Commands.HDFS_WRITE.toString() + Constants.SEPARATOR + name + Constants.SEPARATOR;
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
                        if (read <= 0  || (local.length() == offset + read && Constants.findByte(buf, Constants.NEW_LINE, 0, read) == -1)) {
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
                long minChunkSize = Math.min(chunkSize, local.length() - offset) - prevLineOffset ;


                // Send command header
                byte[] cmd;
                cmd = command.concat(String.valueOf(minChunkSize))
                        .concat(Constants.SEPARATOR)
                        .concat(String.valueOf(chunkSize))
                        .getBytes(StandardCharsets.UTF_8);
                cmd = Arrays.copyOf(cmd, Constants.CMD_BUFFER_SIZE);

                os.write(cmd);

                // Wait for server available space confirmation
                is.readNBytes(cmd, 0, Long.BYTES);
                long res = Constants.getLong(cmd);
                if (res < 0){
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
                        (ind = Constants.findByte(buf, Constants.NEW_LINE, (int)(read - (total - chunkSize)) % size, buf.length)) == -1))
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

                // Check size
                is.readNBytes(cmd, 0,  Long.BYTES);
                long written = Constants.getLong(cmd);

                if (written != totalWritten){
                    // if negative, an error code was return if not it's a size
                    if (written < 0){
                        status = written;
                        System.err.println(serverIp+" ("+id+") WR error "+written);
                    } else {
                        status = Constants.INCONSISTENT_FILE_SIZE;
                        System.err.println(serverIp + " (" + id + ") WR error "+status+" : " + written + " / " + totalWritten);
                    }
                }


                // Close file and connection
                in.close();
                hdfsSocket.close();


                // Success
                return new OperationResult<>(id, serverIp, status);

            } catch (Exception e) {
                System.out.println(serverIp+" ("+id+") : "+e.getMessage());
                e.printStackTrace();
                // Signal failure
                return new OperationResult<>(id, serverIp, Constants.IO_ERROR);
            }
        }
    }



}

package hdfs;

import config.FileData;
import config.AppData;
import config.Metadata;
import config.Project;
import formats.Format;
import java.io.*;
import java.nio.file.FileAlreadyExistsException;
import java.text.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class HdfsClient {
    private static AppData data;
  

    private static void usage() {
        System.out.println("Use: java HdfsClient { -r <file> [localDest] " +
                "| -w <file> [-f ln|kv] [ --chunks-size=<sizeInBytes> ] [ --rep=<repFactor> ] " +
                "| -d <file> " +
                "| -l [-d] }\n"+
                "Default format is ln. \n" +
                "--rep is currently not supported and is always 1.");
    }


    /**
     * Get path for file.
     * If the given file name starts with '/' it is considered as an absolute path
     * and will be used as such. Otherwise, the relative path in $HIDOOP_HOME will
     * be used.
     * @return path for this file
     */
    private static String getPathForFile(String file){
        if (file.trim().charAt(0)=='/') return file; // Absolute path
        else return Project.getDataPath()+file; // Path relative to data folder
    }


    /**
     * Print file list
     */
    public static void HdfsList(boolean details) {
        DateFormat df = new SimpleDateFormat();
        Metadata data = HdfsClient.data.getMetadata();
        FileData fd;
        String size, chunkSize;
        System.out.println("-----------------------------------------\n");
        System.out.println(data.getFileCount()+ " saved (last saving " + df.format(data.getSaveDate())+") : ");

        for (String n : data.getFileNames()){
            fd = data.retrieveFileData(n);
            size = Constants.getHumanReadableSize(fd.getFileSize());
            chunkSize = Constants.getHumanReadableSize(fd.getChunkSize());
            System.out.print("\n    - " + n + " (" + size + ")");

            if (details) {
                System.out.println("      " + fd.getChunkCount() + " * " + chunkSize + " chunks");

                for (int id : fd.getChunksIds()) {
                    System.out.println("        #" + id + " :");
                    for (String ip : fd.getSourcesForChunk(id)) {
                        System.out.println("            - " + ip);
                    }
                }
            } else {
                System.out.println();
            }
        }
        System.out.println("-----------------------------------------");
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
            throws IOException {

        // Control fileName length because of buffers
        if (localFSSourceFname.length() > Constants.MAX_NAME_LENGTH) localFSSourceFname = localFSSourceFname.substring(0,Constants.MAX_NAME_LENGTH);

        // Get metadata and servers location
        Metadata data = getMetadata();
        final String[] SERVERS_IP = HdfsClient.data.getServersIp();

        final File local = new File(getPathForFile(localFSSourceFname));
        localFSSourceFname = local.getName();

        long size = local.length(); // size in bytes

        FileData fd = data.retrieveFileData(localFSSourceFname);
        boolean isNew = (fd == null);

        // Use default size
        if (chunkSize <= 0) chunkSize = HdfsClient.data.getDefaultChunkSize();   // distributed : chunkSize = size / SERVERS_IP.length + (size % SERVERS_IP.length == 0 ? 0 : 1);


        if (isNew) {
            // Create file
            fd = new FileData(fmt, size, chunkSize);

        } else {
            // TODO is append possible?
            throw new FileAlreadyExistsException(localFSSourceFname);
        }
        int count = (int) (size / chunkSize) + (size % chunkSize == 0 ? 0 : 1);

        Writer writer = new Writer(fd, SERVERS_IP, local, chunkSize, count, fmt);

            // Save updated metadata
            if (writer.exec()) {
                HdfsClient.getMetadata().addFileData(localFSSourceFname, fd);
                HdfsClient.data.saveMetadata(data);
                System.out.println(localFSSourceFname + " successfully saved.");
            } else {
                System.out.println("Error writing file.");
            }


    }


    /**
     * Read a file stored in HDFS
     * @param hdfsFname name of the file
     * @param localFSDestFname local dest file
     */
    public static void HdfsRead(String hdfsFname, String localFSDestFname)
            throws IOException {

        if (localFSDestFname == null) localFSDestFname = "r_"+hdfsFname;

        FileData fd = getMetadata().retrieveFileData(hdfsFname);
        if (fd == null) throw new FileNotFoundException(hdfsFname);

        File local = new File(getPathForFile(localFSDestFname));
        localFSDestFname = local.getName();

        System.out.println("Reading file...");

        Reader rd = new Reader(fd, hdfsFname, local);

        if (rd.exec()) System.out.println(hdfsFname + " successfully read to "+localFSDestFname+".");
        else {
            local.deleteOnExit();
            System.out.println("Error reading file.");
        }

    }

    /**
     * Delete file in HDFS
     * @param hdfsFname name of the stored file
     */
    public static void HdfsDelete(String hdfsFname)
            throws IOException {

        Metadata data = getMetadata();
        FileData fd = data.retrieveFileData(hdfsFname);
        if (fd == null) throw new FileNotFoundException(hdfsFname);

        Deleter del = new Deleter(fd, hdfsFname);

        // Save updated metadata
        if (del.exec()) {
            HdfsClient.getMetadata().removeFileData(hdfsFname);
            HdfsClient.data.saveMetadata(data);
            System.out.println(hdfsFname + " successfully deleted.");
        } else {
            System.out.println("Error deleting file.");
        }

    }


    private static Metadata getMetadata() {
        return data.getMetadata();
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
                    boolean details = false;
                    if (args.length > 1) {
                        if (args.length == 2 && args[1].equals("--detail")) details = true;
                        else {
                            usage();
                            return;
                        }
                    }
                    HdfsList(details);
                    System.out.println("-- time (ms) : "+(System.currentTimeMillis() - start));
                    break;
                case "-r":
                    data = AppData.loadConfigAndMeta(false);
                    start = System.currentTimeMillis();
                    HdfsRead(args[1], args.length > 2 ? args[2] : null);
                    System.out.println("-- time (ms) : "+(System.currentTimeMillis() - start));
                    break;
                case "-d":
                    data = AppData.loadConfigAndMeta(false);
                    start = System.currentTimeMillis();
                    HdfsDelete(args[1]);
                    System.out.println("-- time (ms) : "+(System.currentTimeMillis() - start));
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

                            Pattern r = Pattern.compile("(?<size>[0-9]+(\\.[0-9]+)?)(?<unit>B|kB|MB|GB|TB)?");
                            Matcher m = r.matcher(mode);
                            if(m.matches()) {
                                String unit = m.group("unit");
                                if (unit == null) unit = "B";
                                chunksMode = Constants.getSize(Float.parseFloat(m.group("size")), unit);
                            } else {
                                chunksMode = -1;
                            }
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
                    System.out.println("-- time (ms) : "+(System.currentTimeMillis() - start));
                    break;
                default: usage();
            }
        } catch (FileNotFoundException | FileAlreadyExistsException ferr){
            System.out.println(ferr.getClass().getSimpleName()+" : "+ferr.getMessage());
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            ex.printStackTrace();
        }
    }

}


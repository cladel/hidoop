package config;

import formats.Format;

import java.io.Serializable;
import java.util.*;

/**
 * HDFS file data, including the location of the server(s) where a chunk
 * is stored
 * Size is only indicative and is not used to verify files' integrity
 */
public class FileData implements Serializable{
    public static final long serialVersionUID = 1L;

    public static final long UNSPECIFIED_SIZE = -1;

    private long fileSize; // file size in bytes
    private long chunkSize; // chunks size in bytes
    private int rep = 1; // rep count
    private Format.Type format; // format of file
    private HashMap<Integer,ChunkSources> chunks; // location of each chunk


    /**
     * FileData of given format and size
     * @param fmt format
     * @param size size of file
     * @param chunkSize average size of chunks
     */
    public FileData(Format.Type fmt, long size, long chunkSize){
        this.format = fmt;
        this.fileSize = size;
        this.chunkSize = chunkSize;
        this.chunks = new HashMap<>();
    }

    /**
     * FileData of given format and unspecified size (ie getFileSize() = getChunkSize() = -1)
     * Only used when registering a file from the nodes (for example Map results)
     * @param fmt format
     */
    public FileData(Format.Type fmt){
        this(fmt, UNSPECIFIED_SIZE, UNSPECIFIED_SIZE);
    }

    public long getChunkSize() {
        return chunkSize;
    }

    public long getFileSize() {
        return fileSize;
    }

    public int getRepCount() {
        return rep;
    }

    public Format.Type getFormat() {
        return format;
    }

    /**
     * Register chunk handle
     * @param chunkId id of the chunk
     * @param nodeIp ip of server
     */
    public void addChunkHandle(int chunkId, String nodeIp){
        if(chunks.containsKey(chunkId)){
            chunks.get(chunkId).addChunkSource(nodeIp);
        } else {
            ChunkSources cs = new ChunkSources();
            cs.addChunkSource(nodeIp);
            chunks.put(chunkId, cs);
        }
    }

    /**
     * Get number of chunks for this file
     */
    public int getChunkCount(){
        return chunks.size();
    }

    /**
     * Get list of chunk ids
     */
    public ArrayList<Integer> getChunksIds() {
        ArrayList<Integer> idsList = new ArrayList<>(chunks.keySet());
        Collections.sort(idsList);
        return idsList;
    }

    /**
     * Get list of nodes ip for a given chunk
     * @param id id of the chunk
     */
    public List<String> getSourcesForChunk(int id){
        ChunkSources cs = chunks.get(id);
        return cs == null ? null : Collections.unmodifiableList(cs.ipList);
    }

    /**
     * Generate unique chunk name
     * @param id id of the chunk
     * @param fname file name
     * @param fmt format
     * @return chunk name
     */
    public static String chunkName(int id, String fname, Format.Type fmt){
        return id + "_" + fname + (fmt == Format.Type.KV ? ".kv" : ".ln");
    }


    private class ChunkSources implements Serializable {
        public static final long serialVersionUID = FileData.serialVersionUID;
        private final ArrayList<String> ipList;

        ChunkSources(){
            ipList = new ArrayList<>(rep);
        }

        void addChunkSource(String ip){
            if (ipList.size() < rep) ipList.add(ip);
            else throw new IndexOutOfBoundsException("Cannot have more than "+rep+" copies of each chunks.");
        }
    }
}

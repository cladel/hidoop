package config;

import formats.Format;

import java.io.Serializable;
import java.util.*;

/**
 * HDFS file data, including the location of the server(s) where a chunk
 * is stored
 */
public class FileData implements Serializable{
    public static final long serialVersionUID = Metadata.serialVersionUID;

    public static final long UNKNOWN_CHUNK_SIZE = -1;

    private long fileSize; // file size in bytes
    private long chunkSize; // chunks size in bytes
    private int rep = 1;
    private Format.Type format;
    private HashMap<Integer,ChunkSources> chunks;


    public FileData(Format.Type fmt, long size, long chunkSize){
        this.format = fmt;
        this.fileSize = size;
        this.chunkSize = chunkSize;
        this.chunks = new HashMap<>();
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

    public void addChunkHandle(int chunkId, String nodeIp){
        if(chunks.containsKey(chunkId)){
            chunks.get(chunkId).addChunkSource(nodeIp);
        } else {
            ChunkSources cs = new ChunkSources();
            cs.addChunkSource(nodeIp);
            chunks.put(chunkId, cs);
        }
    }

    public int getChunkCount(){
        return chunks.size();
    }

    public ArrayList<Integer> getChunksIds() {
        ArrayList<Integer> idsList = new ArrayList<>(chunks.keySet());
        Collections.sort(idsList);
        return idsList;
    }

    public List<String> getSourcesForChunk(int id){
        return Collections.unmodifiableList(chunks.get(id).ipList);
    }

    public static String chunkName(int id, String fname, Format.Type fmt){
        return id + "_" + fname + (fmt == Format.Type.KV ? ".kv" : ".ln");
    }


    private class ChunkSources implements Serializable {
        public static final long serialVersionUID = Metadata.serialVersionUID;
        private final ArrayList<String> ipList;

        ChunkSources(){
            ipList = new ArrayList<>(rep);
        }

        void addChunkSource(String ip){
            if (ipList.size() < rep) ipList.add(ip);
            //TODO throw exception sinon?
        }
    }
}

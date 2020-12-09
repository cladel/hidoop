package hdfs;

import formats.Format;

import java.io.Serializable;
import java.util.*;

public class FileData implements Serializable{
    public static final long versionID = Metadata.versionID;

    private long fileSize;
    private int chunkSize;
    private int rep = 1;
    private Format.Type format;
    private HashMap<Integer,ChunkSources> chunks;
    private int lastChunkId; // TODO pour gestion de l'append


    public FileData(Format.Type fmt, long size, int chunkSize){
        this.format = fmt;
        this.fileSize = size;
        this.chunkSize = chunkSize;
        this.chunks = new HashMap<>();
    }

    public int getChunkSize() {
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
            fileSize += chunkSize;
            // TODO gestion de chunks ne remplissant pas la taille chunkSize (cas append)
        }
    }

    public int getChunkCount(){
        return chunks.size();
    }

    public Set<Integer> getChunksIds() {
        return chunks.keySet();
    }

    public List<String> getSourcesForChunk(int id){
        return Collections.unmodifiableList(chunks.get(id).ipList);
    }


    private class ChunkSources implements Serializable{
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

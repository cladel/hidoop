package hdfs;

import java.text.DecimalFormat;
import java.util.Arrays;

/**
 * HDFS macros and utils
 */
public class Constants {
    public static final int PORT = 3000;
    protected static final int BUFFER_SIZE = 8 * 1024 * 1024; // 8 MiB
    protected static final int CMD_BUFFER_SIZE = 128; // 4 + file_name + 8 bytes * 2
    protected final static String SEPARATOR = " ";
    protected final static byte END_CHUNK_DELIMITER = 0x03; // End of text
    protected final static byte NEW_LINE = 0x0a; // End of line

    public final static int MAX_NAME_LENGTH = 80; // Max length of file name

    protected final static int CHUNK_LIMIT_FACTOR = 2;

    /* Error constants */
    public final static long FILE_EMPTY = -2;
    public final static long CHUNK_TOO_LARGE = -3;
    public final static long INCONSISTENT_FILE_SIZE = -4;
    public final static long IO_ERROR = -5;
    public final static long FILE_TOO_LARGE = -6;
    public final static long FILE_NOT_FOUND = -1;



    /**
     * Put long at the start of a buffer
     * @param bufCmd the buffer in which to write the long
     * @param val the value to write
     */
    public static void putLong(byte[] bufCmd, long val) {
        Arrays.fill(bufCmd, (byte) 0);
        for (int i = 0; i < 8; i++) {
            bufCmd[i] = (byte) ((val >> 8 * i) & 0xFF);
        }
    }

    /**
     * Read long at the start of a buffer
     * @param bufCmd the buffer in which to read the long
     * @return long
     */
    public static long getLong(byte[] bufCmd) {
        long res = 0;
        for (int i = 0; i < 8; i++) {
            res |= ((long) bufCmd[i] & 0xFF)<< (8 * i);
        }
        return res;
    }

    /**
     * Find next '<code>chr</code>' in an array of bytes
     * @param array characters as array of bytes
     * @param startAt ignore bytes before this index
     * @param end search only before this index
     * @param chr character (byte) to find
     * @return index of first occurrence of '<code>chr</code>', or -1 if not found
     */
    public static int findByte(byte[] array, byte chr, int startAt, int end){
        if (startAt >= array.length || end < startAt) return -1;
        for (int i = startAt; i < end; i++){
            if (array[i] == chr) return i;
        }
        return -1;
    }

    /**
     * Get human readable size (bytes, kB, MB, GB, TB)
     * @param size in bytes
     * @return the formatted size, or "UNKNOWN SIZE" if argument is negative
     */
    public static String getHumanReadableSize(long size){
        if (size < 0) return "UNKNOWN SIZE";
        else if (size < 1000) return  size+" B";
        String prefixes = "kMGT";
        int i = 0;
        float s = size/1000f;
        while (i < prefixes.length() && s >= 1000f){
            s /= 1000f;
            i++;
        }

        return new DecimalFormat("#.#").format(s)+" "+prefixes.charAt(i)+"B";
    }

    /**
     * Get long size from floating point value and unit
     * @param size value
     * @param unit unit bytes, B, kB, MB, GB, TB (case insensitive)
     * @return size as long
     */
    public static long getSize(long size, String unit){
        switch (unit.toUpperCase()){
            case "TB":
                size *= 1000;
            case "GB":
                size *= 1000;
            case "MB":
                size *= 1000;
            case "KB":
                size *= 1000;
            case "B":
            case "BYTES":
                break;
            default:
                size = -1;
                break;
        }
        return size;
    }
}

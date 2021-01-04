package hdfs;

import java.util.Arrays;

/**
 * HDFS macros and utils
 */
public class Constants {
    public static final int PORT = 3000;
    public static final int BUFFER_SIZE = 1024;
    public static final int CMD_BUFFER_SIZE = 96; // 4 + file_name + 8 bytes
    public final static String SEPARATOR = " ";
    public final static byte END_CHUNK_DELIMITER = 0x03; // End of text
    public final static byte NEW_LINE = 0x0a; // End of line

    public final static int MAX_NAME_LENGTH = 80; // Max length of file name

    /* Error constants */
    public final static long FILE_EMPTY = -2;
    public final static long FILE_TOO_LARGE = -3;
    public final static long INCONSISTENT_FILE_SIZE = -4;
    public final static long IO_ERROR = -5;
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
}

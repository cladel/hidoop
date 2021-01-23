package hdfs;


/**
 * Result of an operation with HdfsServer
 * @param <T> type of the result information
 */
class OperationResult<T> {
    private final int id;
    private final String ipSource;
    private final T res;

    OperationResult(int id, String ipSource, T res) {
        this.id = id;
        this.ipSource = ipSource;
        this.res = res;
    }

    public int getId() {
        return id;
    }

    public String getIpSource() {
        return ipSource;
    }

    public T getRes() {
        return res;
    }
}
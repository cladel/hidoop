package hdfs;


import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

public abstract class ClientTask<V> {

    private final int count;
    private boolean abortOnError;
    private final BlockingQueue<Future<OperationResult<V>>> queue;
    private final List<Future<OperationResult<V>>> results;


    private final ExecutorService pool;

    public ClientTask(int count, boolean abortOnError) {
        this.count = count;
        this.abortOnError = abortOnError;
        int procCount = Runtime.getRuntime().availableProcessors();
        pool = Executors.newFixedThreadPool(Math.min(count, procCount));
        queue = new ArrayBlockingQueue<>(count, false);
        results = new LinkedList<>();

    }

    public boolean exec() {
        try {
            // Thread launching operations on the chunks
            pool.submit(new TaskSubmitter());


            // Current thread getting results
            boolean allOk = true;
            for (int i = 0; i < count; i++) {
                Future<OperationResult<V>> b = queue.take();
                results.add(b);
                OperationResult<V> res = b.get();
                allOk = onResult(res); // returns allOk
                if (!allOk && abortOnError) break; // For now if an error occurs, abort

            }

            if (allOk) {
                pool.shutdown();
            } else {
                rollback(results);
            }

            return allOk;
        } catch (ExecutionException | InterruptedException e){
            e.printStackTrace();
            return false;
        }
    }

    void rollback(List<Future<OperationResult<V>>> results){
        pool.shutdownNow();
    }

    abstract Callable<OperationResult<V>> submitTask(int i);

    abstract boolean onResult(OperationResult<V> res);

    private class TaskSubmitter implements Runnable {


        @Override
        public void run() {

            // Submit each chunk to the pool
            for (int i = 0; i < count; i++) {
                Future<OperationResult<V>> b = pool.submit(submitTask(i));
                queue.offer(b);
            }

        }
    }
}

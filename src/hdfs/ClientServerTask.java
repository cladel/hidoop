package hdfs;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Abstract class representing an HDFS client/server task.
 * @param <V> type of the parameter used as an OperationResult
 */
public abstract class ClientServerTask<V> {

    final int count;
    private final boolean abortOnError;
    private final ArrayList<Future<OperationResult<V>>> results;
    private final Semaphore sem = new Semaphore(0);
    private int progress = 0;
    private boolean printProgress = true;


    private final ExecutorService pool;

    public ClientServerTask(int count, boolean abortOnError) {
        this.count = count;
        this.abortOnError = abortOnError;
        int procCount = Math.min(count, Runtime.getRuntime().availableProcessors());
        pool = Executors.newFixedThreadPool(procCount);
        results =  new ArrayList<>(count);

    }

    /**
     * Executes the task
     * @return true if ok, false if error
     */
    public boolean exec() {

        Thread restoreIfStop = new Thread(() -> {
            System.out.println("\nInterrupted.");
            onAbort(results);
        });
        boolean allOk = false;

        if (abortOnError) Runtime.getRuntime().addShutdownHook(restoreIfStop);

        try {

            // Thread launching operations on the chunks
            pool.submit(() -> {
                try {
                    // Submit each chunk to the pool
                    for (int i = 0; i < count; i++) {
                        results.add(pool.submit(submitTask(i)));
                        sem.release();
                    }
                } catch (Exception e){e.printStackTrace();}
            });


            // Current thread getting results
            allOk = true;

            if (printProgress) {
                System.out.print("# 0 %");
                System.out.flush();
            }
            for (int i = 0; i < count; i++) {

                sem.acquire();
                Future<OperationResult<V>> b = results.get(i);
                OperationResult<V> res = b.get();
                allOk = onResult(res); // returns allOk
                if (allOk){
                    if (printProgress) {
                        System.out.print("\r# " + getProgress() + " %");
                        System.out.flush();
                    }
                } else if (abortOnError) break; // For now if an error occurs, abort

            }
            System.out.println();

            if (allOk) {
                pool.shutdown();
            } else {
                onAbort(results);
            }
            Runtime.getRuntime().removeShutdownHook(restoreIfStop);


        } catch (ExecutionException | InterruptedException e){
            e.printStackTrace();
        }
        return allOk;
    }

    /**
     * Action to cancel current task and rollback to previous hdfs state.
     * @param tasks tasks scheduled
     */
    void onAbort(List<Future<OperationResult<V>>> tasks){
        pool.shutdownNow();
    }

    /**
     * Generate a subtask associated with a particular chunk.
     * @param i chunk number
     * @return subtask
     */
    abstract Callable<OperationResult<V>> submitTask(int i);

    /**
     * Called when a subtask's result is obtained.
     * @param res result of the task
     * @return true if it's ok, false otherwise
     */
    abstract boolean onResult(OperationResult<V> res);


    int getProgress(){
        return (++progress * 100 / count);
    }

    public void setPrintProgress(boolean printProgress) {
        this.printProgress = printProgress;
    }

    public boolean printsProgress() {
        return printProgress;
    }

}

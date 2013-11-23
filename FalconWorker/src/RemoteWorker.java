import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class RemoteWorker {
	/**
	 * 
	 * @param args			poolSize, processMaxCount
	 */
    public static void main(String[] args) {
    	int poolSize = Integer.valueOf(args[0]);
    	int processMaxCount = Integer.valueOf(args[1]);
    	ExecutorService  pool = Executors.newFixedThreadPool(poolSize);
    	for (int i = 0; i < poolSize; i++) {
    	    pool.submit(new WorkerThread(processMaxCount));
    	}
    	
    	pool.shutdown();
    }
}

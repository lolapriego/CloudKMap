import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class RemoteWorker {

    public static void main(String[] args) {
    	int poolSize = Integer.valueOf(args[0]);
    	int processMaxCount = Integer.valueOf(args[1]);
    	boolean duplicateCheck = Boolean.valueOf(args[2]); 
    	boolean monitoring = Boolean.valueOf(args[3]);
    	ExecutorService  pool = Executors.newFixedThreadPool(poolSize);
    	for (int i = 0; i < poolSize; i++) {
    	    pool.submit(new WorkerThread(processMaxCount,duplicateCheck,monitoring));
    	}
    	
    	pool.shutdown();
    }
}

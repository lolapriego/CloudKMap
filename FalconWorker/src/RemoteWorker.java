package src;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class RemoteWorker {
	/**
	 * 
	 * @param args			poolSize, processMaxCount
	 * @throws BrokenBarrierException 
	 * @throws InterruptedException 
	 */
    public static void main(String[] args) throws InterruptedException, BrokenBarrierException {
    	int poolSize = Integer.valueOf(args[0]);
    	int processMaxCount = Integer.valueOf(args[1]);
    	CyclicBarrier barrier = new CyclicBarrier(poolSize);
    	ExecutorService  pool = Executors.newFixedThreadPool(poolSize);
    	
  	
    	for (int i = 0; i < poolSize; i++) {
    	    pool.submit(new WorkerThread(processMaxCount));
    	}

    	barrier.await();
    	pool.shutdown();
	    
    }
}

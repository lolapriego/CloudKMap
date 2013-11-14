import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;

public class FalconClient {
	static String QueueUrlPrefix="https://sqs.us-east-1.amazonaws.com"
    		+"/728278020921/"; //728278020921
	static ConcurrentHashMap<Long, Task> completeTaskList = new ConcurrentHashMap<Long,Task>();
	 static CyclicBarrier barrier = null; // need to initialize number of threads later
	 
    public static void main(String[] args) throws InterruptedException, BrokenBarrierException {
    	long timing;
    	String url = null;
    	String clientId =  UUID.randomUUID().toString();
    	int threadCount = Integer.valueOf(args[0]);
    	AmazonSQS sqs = new AmazonSQSClient(new ClasspathPropertiesFileCredentialsProvider());
    	barrier =  new CyclicBarrier(threadCount+1);
    	timing = System.currentTimeMillis();
    	//create response queue for this client
        try {
			Region usEast1 = Region.getRegion(Regions.US_EAST_1);
			sqs.setRegion(usEast1);
	    	CreateQueueRequest createQueueRequest = new CreateQueueRequest(clientId);
	         url = sqs.createQueue(createQueueRequest).getQueueUrl();
		} catch (AmazonServiceException ase) {
	        System.out.println("Amazon Internal Error:");
	        System.out.println("Error Message:    " + ase.getMessage());
	        System.out.println("HTTP Status Code: " + ase.getStatusCode());
	        System.out.println("AWS Error Code:   " + ase.getErrorCode());
	        System.out.println("Error Type:       " + ase.getErrorType());
	        System.out.println("Request ID:       " + ase.getRequestId());
		    } catch (AmazonClientException ace) {
		        System.out.println("SQS Internal Error.");
		        System.out.println("Error Message: " + ace.getMessage());
		    } 
        
        // run the client threads to send tasks

    	ExecutorService  pool = Executors.newFixedThreadPool(threadCount);

    	for (int i = 0; i < threadCount; i++) {
    		pool.submit(new ClientThread(msgCount,threadCount,clientId,sleepLength));
    	}
    	barrier.await();// waits for threads to finish!
    	pool.shutdown();
    	timing = System.currentTimeMillis()-timing;    	

    	Enumeration<Long> en=completeTaskList.keys();
    	Task tsk;
    	Long currentKey;
    	try {

			File file = new File("output");
			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
	    	while(en.hasMoreElements()){
	    		currentKey = en.nextElement();
	    		tsk = completeTaskList.get(currentKey);		
	    		bw.write(String.valueOf(tsk.getTaskId())+" "+tsk.getSendTime()+" "
	    		+tsk.getReceiveTime()+" "+tsk.getCompleteTime()+" "+tsk.getFinishTime()+"\n");
			}
	    	bw.flush();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
    	//delete the response queue after it's over. not enabled yet
        sqs.deleteQueue(new DeleteQueueRequest(url));
    	System.out.println("total time: "+timing);
    	System.out.println("throughput: "+1000*threadCount*msgCount/timing);
    }
}

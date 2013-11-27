import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
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
import com.cloudmap.message.TaskMessage.Task;

public class FalconClient {
	static ConcurrentHashMap<Long, Task> completeTaskList = new ConcurrentHashMap<Long,Task>();
	static CyclicBarrier barrier = null; // need to initialize number of threads later
	static Set<String> keyList = new HashSet<String>();

    public static void main(String[] args) throws InterruptedException, BrokenBarrierException {
        int threadCount = Integer.valueOf(args[0]);
    	Splitter splitter = new Splitter(args[1]);
     	List<String> inputPaths;

    	long timing = System.currentTimeMillis();
    	String clientId =  UUID.randomUUID().toString();
    	barrier =  new CyclicBarrier(threadCount+1);

        String urlRequests = null;
        String urlResponses = null;

        AmazonSQS sqs = new AmazonSQSClient(new ClasspathPropertiesFileCredentialsProvider());

        // The list will record input paths for each thread at map stage
        // Or a list of keys for the reduce stage
        boolean mapType = true;
        List<String> input;

    	//create response queue for this client
        try {
			Region usEast1 = Region.getRegion(Regions.US_EAST_1);
			sqs.setRegion(usEast1);
	    	CreateQueueRequest createQueueRequest = new CreateQueueRequest("TaskQueue");
	    	CreateQueueRequest createQueueResponse = new CreateQueueRequest(clientId);
	        urlRequests = sqs.createQueue(createQueueRequest).getQueueUrl();
	        urlResponses = sqs.createQueue(createQueueResponse).getQueueUrl();

	        System.out.println("Queues created");
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
		inputPaths = splitter.inputSplitter();
    	ExecutorService  pool = Executors.newFixedThreadPool(threadCount);

    	int nBagTasks = inputPaths.size()/threadCount;
        for(int i = 0; i < threadCount - 1; i++){
        	input = new ArrayList<String>();
        	for (int j = nBagTasks * i; j < nBagTasks * (i+1); j++) {
        	    input.add(inputPaths.get(j));
        	}
        	pool.submit(new ClientThread(threadCount,clientId, input, mapType));
        }

    	input = new ArrayList<String>();
        for(int i = (threadCount - 1) * nBagTasks; i < inputPaths.size(); i++){
    	    input.add(inputPaths.get(i));
        }
        pool.submit(new ClientThread(threadCount,clientId, input, mapType));

    	barrier.await();// waits for threads to finish!
    	pool.shutdown();
    	timing = System.currentTimeMillis()-timing;

    	System.out.println("============= Started the Reduce Stage =============");
    	mapType = false;

    	// Launch reduce tasks
    	// Take the number of keys produced. And the folder path
    	completeTaskList.clear();
    	barrier =  new CyclicBarrier(threadCount+1);
    	timing = System.currentTimeMillis();

        // run the client threads to send tasks
    	pool = Executors.newFixedThreadPool(threadCount);

    	nBagTasks = keyList.size()/threadCount;
    	Iterator<String> iterator = keyList.iterator();
    	for(int i = 0; i < threadCount - 1; i++){
    		input = new ArrayList<String>();
        	for (int j = nBagTasks * i; j < nBagTasks * (i+1); j++) {
    			input.add(iterator.next());
    		}
    		pool.submit(new ClientThread(threadCount, clientId, input, mapType));
    	}

    	input = new ArrayList<String>();
    	for(int i = (threadCount - 1) * nBagTasks; i < keyList.size(); i++){
    		input.add(iterator.next());
    	}
    	pool.submit(new ClientThread(threadCount, clientId, input, mapType));

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
	    		bw.write(
	    		"Id: " + String.valueOf(tsk.getTaskId()) +
	    		"Send time of the Reduce task: "+tsk.getSendTime() +
	    		"Receive Time: " + tsk.getReceiveTime() +
	    		"Complete Time: " + tsk.getCompleteTime() +
	    		"Finish Time: " + tsk.getFinishTime() +
	    		"Key Processed: " + tsk.getKeys() );
			}
	    	bw.flush();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
    	//delete the response queue after it's over. not enabled yet
        //sqs.deleteQueue(new DeleteQueueRequest(urlRequests));
        sqs.deleteQueue(new DeleteQueueRequest(urlResponses));

    	System.out.println("total time: "+timing);
    	System.out.println("throughput: "+1000*threadCount*inputPaths.size()*2/timing);
    }
}

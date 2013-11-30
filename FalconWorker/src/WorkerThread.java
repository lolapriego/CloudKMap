package src;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.springframework.security.crypto.codec.Base64;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.google.protobuf.InvalidProtocolBufferException;
import com.cloudmap.message.TaskMessage.Task;

public class WorkerThread implements Runnable{
	
	boolean isBusy = false;
	boolean isDone = false;
	AmazonSQS sqs;
	String QueueUrlPrefix=null;
	
	static String tableName = "messages";
	Collection<String> attributeNames;
	static String workerId;
	public int processMaxCount;
	String requestQueueUrl;
	
	public String threadFlag;
	
	/**
	 * Worker Thread Constructor
	 * @param processMaxCount
	 */
	public WorkerThread(int processMaxCount) {
		// Setup SQS
		sqs = new AmazonSQSClient(new ClasspathPropertiesFileCredentialsProvider());
		Region usEast1 = Region.getRegion(Regions.US_EAST_1);
		sqs.setRegion(usEast1);

		// Setup attributes
		this.attributeNames = new ArrayList<String>();
		this.attributeNames.add("ApproximateFirstReceiveTimestamp");
		this.attributeNames.add("SentTimestamp");
		workerId =  UUID.randomUUID().toString();
		this.processMaxCount = processMaxCount;
		
		// Get SQS
		GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest("TaskQueue");
        requestQueueUrl = sqs.getQueueUrl(getQueueUrlRequest).getQueueUrl();		
        //QueueUrlPrefix=requestQueueUrl.substring(0,requestQueueUrl.lastIndexOf('/')+1);
	}

	/**
	 * Send Response back to SQS
	 * 
	 * @param task		task to be dumped		
	 */
	private void sendReponse(Task.Builder task, String responseQueueName){
		
		GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(responseQueueName);
		String responseQueueUrl = sqs.getQueueUrl(getQueueUrlRequest).getQueueUrl();
		String stringTask = new String(Base64.encode(task.build().toByteArray()));
		
		System.out.println("Sending response to " + responseQueueUrl);
        sqs.sendMessage(new SendMessageRequest(responseQueueUrl, stringTask));
	}
	
	/**
	 * Get task queue length
	 * 
	 * @param queueUrl
	 * @return
	 */
	public int getQueueLength(String queueUrl){
		HashMap<String, String> attributes;
		sqs = new AmazonSQSClient(new ClasspathPropertiesFileCredentialsProvider());
		Region usEast1 = Region.getRegion(Regions.US_EAST_1);
		sqs.setRegion(usEast1);
		Collection<String> attributeNames = new ArrayList<String>();
		attributeNames.add("ApproximateNumberOfMessages");
		GetQueueAttributesRequest getAttributesRequest = new GetQueueAttributesRequest(queueUrl)
		.withAttributeNames(attributeNames);
		attributes = (HashMap<String, String>) sqs.getQueueAttributes(getAttributesRequest).getAttributes();
		
		return Integer.valueOf(attributes.get("ApproximateNumberOfMessages"));
	}
	
	/**
	 * Pull and delete task?
	 *  
	 */
	public void pullAndDelete(){
        // Receive 1 messages at most

		byte[] byteTask;
		String msg;
		HashMap<String, String> attributes;
		int isEmpty=0;
        String messageRecieptHandle;
        Task.Builder task = Task.newBuilder();
        
        try{
		   while (isEmpty < 100) { //keeps fetching it's empty.
		        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(requestQueueUrl).withMaxNumberOfMessages(processMaxCount);
		        long receiveTime = System.currentTimeMillis();
		        receiveMessageRequest.setAttributeNames(attributeNames);
		        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
		        
		        if (!messages.isEmpty()) {
		        	
		        	for (int i = 0; i < messages.size(); i++) {
				        messageRecieptHandle = messages.get(i).getReceiptHandle();
				        msg = messages.get(i).getBody();
				        attributes = (HashMap<String, String>) messages.get(i).getAttributes();
				        //delete 1 msg
			            sqs.deleteMessage(new DeleteMessageRequest(requestQueueUrl, messageRecieptHandle));	            
			            // retrieve Task
				        byteTask = Base64.decode(msg.getBytes()); 				        
				        task.mergeFrom(byteTask);
				        
						/*
						 * Parse message and do map/reduce
						 */
						isBusy = true;
						
						boolean taskType = task.getTaskType();
						//TODO: Client need to tell bucket name
						//String bucketName = task.getBucketName();
						String bucketName;
						String splitName = task.getSplitName();
						
						// Do map
						// TODO: Think over a better way to return necessary info
						if(taskType) {
							System.out.println("============= Start Map =============" + threadFlag);
							bucketName = "ckinput";
							
							/*
							 * Setup s3 & read every split
							 * Need to be directly referred,
							 * Otherwise will be closed by GC 
							 */
							ClientConfiguration config = new ClientConfiguration();
							config.setSocketTimeout(0);
							
					        AmazonS3 s3 = new AmazonS3Client(new ClasspathPropertiesFileCredentialsProvider(), config);
							Region usEast1 = Region.getRegion(Regions.US_EAST_1);
							s3.setRegion(usEast1);
					        System.out.println("Loading the bucket: " + bucketName + "|||" + splitName);
					        S3Object object = s3.getObject(new GetObjectRequest(bucketName, splitName));
					        InputStream input = object.getObjectContent();
							
							//InputStream input = RecordHandler.LoadSplit(BucketName, Split);
					        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
						    
					        // Init load buffer
					        ArrayList<String> buffer = new ArrayList<String>();
					        
					        // Loading data chunk to buffer
					        while (true) {
					        	String line = reader.readLine();
					        	if(line == null) break;
					        	
					        	buffer.add(line);
					        	
					        	// For amazon s3 wrapper
					        	AmazonS3 tmp = s3;
					        }
					        reader.close();
					        
					        // Start map
							WordCountMap map = new WordCountMap(bucketName, splitName,buffer);
							
							//task.setTaskType(false);
							task.setKeys(map.getKeyId());
							task.setSplitName(map.getFileList());
						}
						
						// Do reduce
						else {
							// Reduce processes several split results
							System.out.println("\n============= Start Reduce =============" + threadFlag);
							bucketName = "ckmapresults";
							String[] splitKeys = task.getKeys().split(",");
							
							// Get split names from split key
							ArrayList<String> splits = RecordHandler.getSplit(bucketName, splitKeys);
							
							for(String split:splits) {
								
								/*
								 * Setup s3 & read every split
								 * Need to be directly referred,
								 * Otherwise will be closed by GC
								 */
						        AmazonS3 s3 = new AmazonS3Client(new ClasspathPropertiesFileCredentialsProvider());
								Region usEast1 = Region.getRegion(Regions.US_EAST_1);
								s3.setRegion(usEast1);
						        System.out.println("Loading the bucket: " + bucketName + "|||" + split);
						        S3Object object = s3.getObject(new GetObjectRequest(bucketName, split));
						        InputStream input = object.getObjectContent();
								
								//InputStream input = RecordHandler.LoadSplit(bucketName, split);
								BufferedReader reader = new BufferedReader(new InputStreamReader(input));
								
								// Init load buffer
						        ArrayList<String> buffer = new ArrayList<String>();
								
								//Start loading data to buffer
						        while (true) {
						            String line = reader.readLine();
						            if (line == null) break;
						            
						            buffer.add(line);
						            
						        	// For amazon s3 wrapper
						        	AmazonS3 tmp = s3;
						        }
						        reader.close();
							
						        new WordCountReduce(bucketName, splitKeys, buffer);
							}
						}

						isBusy = false;

						//set the time
						task.setReceiveTime(receiveTime);
				        //task.setSendTime(Long.valueOf(attributes.get("SentTimestamp")));
				        task.setCompleteTime(System.currentTimeMillis());						
				        //Done! send the response
				        sendReponse(task, task.getClientId());
					}
				}
		        else if(isEmpty<100 && (getQueueLength(requestQueueUrl) > 0)) {
		        	
				}else{
					isEmpty++;
					try {
						Thread.sleep(500);	
					} catch (Exception e) {
						e.printStackTrace();
					}
					isDone = true;
				}
		   }
	        } catch (AmazonServiceException ase) {
	        System.out.println("Caught an AmazonServiceException, which means your request made it " +
	                "to Amazon SQS, but was rejected with an error response for some reason.");
	        System.out.println("Error Message:    " + ase.getMessage());
	        System.out.println("HTTP Status Code: " + ase.getStatusCode());
	        System.out.println("AWS Error Code:   " + ase.getErrorCode());
	        System.out.println("Error Type:       " + ase.getErrorType());
	        System.out.println("Request ID:       " + ase.getRequestId());
		    } catch (AmazonClientException ace) {
		        System.out.println("Caught an AmazonClientException, which means the client encountered " +
		                "a serious internal problem while trying to communicate with SQS, such as not " +
		                "being able to access the network.");
		        System.out.println("Error Message: " + ace.getMessage());
		    } catch (InvalidProtocolBufferException e) {
				e.printStackTrace();
			} catch(IOException ex) {
				ex.printStackTrace();
			}

	}
	
	/**
	 * For testing
	 * simulate assigning a task message
	 */
	public void Test() {
		Task.Builder task;
		task = Task.newBuilder();
	    task.setClientId("99");
        task.setTaskId(99);//MAX taskcount=100k for thread! =1M per client
        long sendTime = System.currentTimeMillis();
        task.setSendTime(sendTime);
        task.setTaskType(true);
        task.setBucketName("ckinput");
        task.setSplitName("words_ext_0.txt");
        //task.setBucketName("ckmapresults");
        //task.setKeys("co_0.txt");
        task.setResponseQueueUrl("");

        sendReponse(task, "TaskQueue");
	}
	
	@Override
	public void run() {
		// For testing
		//Test();
		
		// TODO: Put somewhere else
		long threadId = Thread.currentThread().getId();
		threadFlag = String.format("============= Thread<< %s >> =============", threadId);
		
		// Pull task and delete
		pullAndDelete();
	}
}
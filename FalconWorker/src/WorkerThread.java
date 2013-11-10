import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.springframework.security.crypto.codec.Base64;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

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
	
	/**
	 * Worker Thread Constructor
	 * @param processMaxCount
	 * @param duplicateCheck
	 * @param monitoring
	 */
	public WorkerThread(int processMaxCount,boolean duplicateCheck,boolean monitoring) {
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
		// TODO: change to our SQS
		GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest("ThroughputMeasure");
        requestQueueUrl = sqs.getQueueUrl(getQueueUrlRequest).getQueueUrl();		
        QueueUrlPrefix=requestQueueUrl.substring(0,requestQueueUrl.lastIndexOf('/')+1);
	}

	/**
	 * Send Response back to SQS
	 * 
	 * @param task		task to be dumped		
	 */
	private void sendReponse(Task.Builder task){
		   
		String stringTask = new String(Base64.encode(task.build().toByteArray()));
        sqs.sendMessage(new SendMessageRequest(QueueUrlPrefix+task.getClientId(), stringTask));
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
	 * @param duplicateCheck
	 */
	public void pullAndDelete(){
        // Receive 1 messages at most

		byte[] byteTask;
		String msg;
		HashMap<String, String> attributes;
		boolean isEmpty=false;
        String messageRecieptHandle;
        Task.Builder task = Task.newBuilder();
        try{
		   while (!isEmpty) { //keeps fetching it's empty.
			    
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
			            sqs.deleteMessage(new DeleteMessageRequest( requestQueueUrl, messageRecieptHandle));	            
			            // retrieve Task
				        byteTask = Base64.decode(msg.getBytes()); 				        
				        task.mergeFrom(byteTask);
				        
						isBusy = true;
						/**
						 * TODO map/reduce
						 */
						isBusy = false;
				        //set the time
						task.setReceiveTime(receiveTime);
				        //task.setSendTime(Long.valueOf(attributes.get("SentTimestamp")));
				        task.setCompleteTime(System.currentTimeMillis());						
				        //Done! send the response
				        sendReponse(task);
					}
				}
		        else if(isEmpty==false && (getQueueLength(requestQueueUrl) > 0)) {
		        	
				}else{
					isEmpty=true;
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
	}
	
	@Override
	public void run() {
		pullAndDelete();
	}
}
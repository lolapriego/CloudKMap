import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.security.crypto.codec.Base64;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.google.protobuf.InvalidProtocolBufferException;
import com.iman.scheduler.message.TaskMessage.Task;


public class WorkerThread implements Runnable{
	
	int duplicate = 0;
	boolean isBusy = false;
	boolean isDone = false;
	boolean duplicateCheck = false;
	boolean monitoring = false;
	AmazonSQS sqs;
	String QueueUrlPrefix=null;
	
	
	static String tableName = "messages";
	static AmazonDynamoDBClient dynamoDB;
	Collection<String> attributeNames;
	static String workerId;
	public int processMaxCount;
	String requestQueueUrl;
	public WorkerThread(int processMaxCount,boolean duplicateCheck,boolean monitoring) {
		sqs = new AmazonSQSClient(new ClasspathPropertiesFileCredentialsProvider());
		Region usEast1 = Region.getRegion(Regions.US_EAST_1);
		sqs.setRegion(usEast1);
		dynamoDB = new AmazonDynamoDBClient(new ClasspathPropertiesFileCredentialsProvider());
        dynamoDB.setRegion(usEast1);
		this.attributeNames = new ArrayList<String>();
		this.attributeNames.add("ApproximateFirstReceiveTimestamp");
		this.attributeNames.add("SentTimestamp");
		workerId =  UUID.randomUUID().toString();
		this.processMaxCount = processMaxCount;
		this.duplicateCheck = duplicateCheck;
		this.monitoring = monitoring;
		GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest("ThroughputMeasure");
        requestQueueUrl = sqs.getQueueUrl(getQueueUrlRequest).getQueueUrl();		
        QueueUrlPrefix=requestQueueUrl.substring(0,requestQueueUrl.lastIndexOf('/')+1);
	}
	private void threadSleep(long sleepLength) {
		try {
			//System.out.println(isBusy);
			//System.out.println("going to sleep for "+ sleepLength);
			Thread.sleep(sleepLength);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private void sendReponse(Task.Builder task){
		   
		String stringTask = new String(Base64.encode(task.build().toByteArray()));
        sqs.sendMessage(new SendMessageRequest(QueueUrlPrefix+task.getClientId(), stringTask));
	}
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
	public void pullAndDelete(boolean duplicateCheck){
        // Receive 1 messages at most

		byte[] byteTask;
		String msg;
		HashMap<String, String> attributes;
		boolean isEmpty=false;
		boolean wasEmpty = false;
		boolean secEmpty = false;
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
				        
				        if (duplicateCheck) {
				        	 // check with dynamoDB and run job if not duplicate
					        try {
						        addItem(tableName, task.getClientId(), task.getTaskId(), workerId);// put it in ddb
								isBusy = true;
						        threadSleep(Long.valueOf(task.getBody()));//assuming task body is sleep length
								isBusy = false;
						        //set the time
								task.setReceiveTime(receiveTime);
						        //task.setSendTime(Long.valueOf(attributes.get("SentTimestamp")));
						        task.setCompleteTime(System.currentTimeMillis());						
						        //Done! send the response
						        sendReponse(task);
							} catch (ConditionalCheckFailedException e) {
								duplicate++;
							}	
						} else {// no duplicate check
							isBusy = true;
							threadSleep(Long.valueOf(task.getBody()));//assuming task body is sleep length
							isBusy = false;
					        //set the time
							task.setReceiveTime(receiveTime);
					        //task.setSendTime(Long.valueOf(attributes.get("SentTimestamp")));
					        task.setCompleteTime(System.currentTimeMillis());						
					        //Done! send the response
					        sendReponse(task);
						}
 
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
		
		if (monitoring) {
		    Thread monitorThread = new Thread(new MonitorThread());
		    monitorThread.start();
			pullAndDelete(duplicateCheck);
			if (duplicateCheck) {
				System.out.println("duplicates #:"+ duplicate);
			}
			try {
				monitorThread.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}else {
			pullAndDelete(duplicateCheck);
			if (duplicateCheck) {
				System.out.println("duplicates #:"+ duplicate);
			}
		}


	}
//    private static String getItemWorkerId(String tableName, String clientId, long taskId){//not used anymore
//        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
//        item.put("messageId", new AttributeValue(clientId+String.valueOf(taskId)));
//        GetItemRequest getItemRequest = new GetItemRequest(tableName, item);
//        GetItemResult getItemResult = dynamoDB.getItem(getItemRequest);
//        if (!getItemResult.toString().equals("{}")){
//        	item= getItemResult.getItem();
//        	return item.get("workerId").getS();
//        } else
//        	return "null";
//    }
    private static void updateMonitor(String tableName,boolean isBusy) {//only busy for now
    	Map<String, AttributeValueUpdate> updateItems = new HashMap<String, AttributeValueUpdate>();
    	HashMap<String, AttributeValue> key = new HashMap<String, AttributeValue>();
    	key.put("id", new AttributeValue().withN("1"));
    	if (isBusy) {
        	updateItems.put("busy", new AttributeValueUpdate().withAction(AttributeAction.ADD)
      			    .withValue(new AttributeValue().withN("+1")));	
		} else {
			updateItems.put("free", new AttributeValueUpdate().withAction(AttributeAction.ADD)
      			    .withValue(new AttributeValue().withN("+1")));	
		}
        
    	UpdateItemRequest updateItemRequest = new UpdateItemRequest().withTableName(tableName)
        .withAttributeUpdates(updateItems);
        	            
        dynamoDB.updateItem(updateItemRequest);
    }
	private static void addItem(String tableName, String clientId, long taskId, String workerId) {
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("messageId", new AttributeValue(clientId+String.valueOf(taskId)));
        item.put("workerId", new AttributeValue(workerId));
        ExpectedAttributeValue notExpected = new ExpectedAttributeValue(false);
        Map<String, ExpectedAttributeValue> expected = new HashMap<String, ExpectedAttributeValue>();
        expected.put("messageId", notExpected);
        PutItemRequest putItemRequest = new PutItemRequest().withTableName(tableName)
        		.withItem(item).withExpected(expected);
         dynamoDB.putItem(putItemRequest);
    }
    public class MonitorThread implements Runnable{

		@Override
		public void run() {
			
			try {
				while (!isDone){
					updateMonitor("monitor", isBusy);
					Thread.sleep(1000);
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
    	
    }

}
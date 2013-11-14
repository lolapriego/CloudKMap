import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageRequest;


public class ClientThread implements Runnable{
	long threadId;

	static String pushQueueUrl="https://sqs.us-east-1.amazonaws.com"
    		+"/728278020921/ThroughputMeasure"; // same as requestqueueurl
	String clientId;// class level client id. same for all threads of this class
	static String tableName = "responseMessages";
	int threadCount; // number of threads 
	int respMsgMaxCount = 10;
	Region usEast1;
	Task.Builder task;
	List<String> data;

	public  ClientThread(int threadCount,String clientId, ArrayList<String> data) {
		this.threadCount = threadCount;
		this.clientId = clientId;
		this.data = data;
		this.task = Task.newBuilder();
		this.usEast1 = Region.getRegion(Regions.US_EAST_1);
	}

	public void pullResponse(AmazonSQS sqs){
        // Receive 10 messages at most
		byte[] byteTask;
		String msg;
		boolean isEmpty=false;
        String messageRecieptHandle;
        Task.Builder task = Task.newBuilder();
        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(clientId);
        String responseUrl = sqs.getQueueUrl(getQueueUrlRequest).getQueueUrl();
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(responseUrl).withMaxNumberOfMessages(respMsgMaxCount);

        List<Message> messages = null;
        try{
		   while (!isEmpty) { //keeps fetching respMsgMaxCount msgs until it's empty.
		        messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
		        long finishTime = System.currentTimeMillis();
		        if (!messages.isEmpty()) {
		        	for (int i = 0; i < messages.size(); i++) {
				        messageRecieptHandle = messages.get(i).getReceiptHandle();
				        msg = messages.get(i).getBody();
				        //delete 1 msg
			            sqs.deleteMessage(new DeleteMessageRequest(responseUrl, messageRecieptHandle));

			            //decode and do something with the msg!!!
			            byteTask = Base64.decode(msg.getBytes());
				        task.mergeFrom(byteTask);// retrieve Task

					    task.setFinishTime(finishTime);// when the message was received
					    FalconClient.completeTaskList.put(task.getTaskId(), task.build());
					}
				} else if(FalconClient.completeTaskList.size() >= data.size()*threadCount ){ // try again to see if something is there!!
					isEmpty = true;
				}
		   }
	        } catch (AmazonServiceException ase) {
	        System.out.println("internal.");
	        System.out.println("Error Message:    " + ase.getMessage());
	        System.out.println("HTTP Status Code: " + ase.getStatusCode());
	        System.out.println("AWS Error Code:   " + ase.getErrorCode());
	        System.out.println("Error Type:       " + ase.getErrorType());
	        System.out.println("Request ID:       " + ase.getRequestId());
		    } catch (AmazonClientException ace) {
		        System.out.println("internal error.");
		        System.out.println("Error Message: " + ace.getMessage());
		    } catch (InvalidProtocolBufferException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

	}

	public void sendRequests(AmazonSQS sqs){
		GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest("ThroughputMeasure");
        String requestQueueUrl = sqs.getQueueUrl(getQueueUrlRequest).getQueueUrl();
        long startTime,sendTime;
		byte[] encoded;
		int i= 0;

		try {
					while (i < data.size()) {
						List<SendMessageBatchRequestEntry> entries = new ArrayList<SendMessageBatchRequestEntry>();
						if (data.size() - i >= 10) {
							for (int j = 0; j < 10; j++) {
								task.setClientId(clientId);
								task.setTaskId(threadId*100000+i);//MAX taskcount=100k for thread! =1M per client
								task.setSplitUrl(data.get(i)); // ??
								sendTime = System.currentTimeMillis();
								task.setSendTime(sendTime);
								encoded = task.build().toByteArray();
								String stringTask = new String(Base64.encode(encoded));

								entries.add(new SendMessageBatchRequestEntry(String.valueOf(i),stringTask));
								i++;
							}
						} else {
							for (int j = 0; j < listTasks.size() - i; j++) {
								task.setClientId(clientId);
								task.setSplitUrl(data.get(i + j));
								task.setTaskId(threadId*100000+i+j);//MAX taskcount=100k for thread! =1M per client
								sendTime = System.currentTimeMillis();
								task.setSendTime(sendTime);
								encoded = task.build().toByteArray();
								String stringTask = new String(Base64.encode(encoded));

								entries.add(new SendMessageBatchRequestEntry(String.valueOf(i+j),stringTask));
							}
							i=data.size();
						}


							SendMessageBatchRequest msgBatch = new SendMessageBatchRequest(requestQueueUrl, entries);
					        sqs.sendMessageBatch(msgBatch);
							//sqs.sendMessage(new SendMessageRequest(pushQueueUrl, stringTask));
					        //System.out.println("task ID: "+task.getTaskId());

					        //FalconClient.completeTasksList.put(threadId*100000+i, false); will be added at the end. not used anymore
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
			        System.out.println("SQS Internal Error.");
			        System.out.println("Error Message: " + ace.getMessage());
			}
	}
	@Override
	public void run() throws AmazonServiceException{
		//each thread has its own sqs object
		threadId = Thread.currentThread().getId();
		AmazonSQS sqs = new AmazonSQSClient(new ClasspathPropertiesFileCredentialsProvider());
		sqs.setRegion(usEast1);
		//send messages
		sendRequests(sqs);
		try {
			Thread.sleep(1000);
			pullResponse(sqs);
			FalconClient.barrier.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			e.printStackTrace();
		}

	}

}

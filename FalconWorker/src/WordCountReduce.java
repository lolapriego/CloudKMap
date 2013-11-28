
import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

/**
 * WordCount Reducer
 * @author hsong
 *
 */
public class WordCountReduce {

	protected String bucketName;
	protected String[] splitKeys;
	protected Hashtable<String, Integer> newKey;
	protected Set<String> fileList; 
	protected ArrayList<String> buffer;				//Buffer for loading s3 object
	
	/**
	 * WordCountReduce Constructor
	 * @param bucketName		bucket name
	 * @param splitKeys			split keys, need to retrieve split name from the key
	 * @throws Exception 
	 */
	public WordCountReduce(String bucketName, String[] splitKeys, ArrayList<String> buffer) throws IOException {
		this.bucketName = bucketName;
		this.splitKeys = splitKeys;
		this.newKey = new Hashtable<String,Integer>();
		this.fileList = new HashSet<String>();
		this.buffer = buffer;
		run();
	}
	
	/**
	 * Main method of reduce count reduce
	 * 
	 * loads data stream from S3 and counts the word, 
	 * then emit count result to separated files for each word
	 * write files back to S3  
	 */
	public void run() throws IOException{
		
	    //Start reduce
        for(String line:buffer) {
            // Parse line
            String[] tmp = line.split(",");
            String key = tmp[0];
            String value = tmp[1];
            
            // Start reduce
            reduce(key, value);
        }
        
        /*
         * Output and store result for this split
         */
        for(String k:newKey.keySet()) {
        	System.out.println(k + " " + newKey.get(k));
        	
        	// Create new fileid
        	String fileId = k.substring(0,2);
        	fileList.add(fileId);
			
        	// Append the value to emit file
			File file = new File(fileId);
			file.deleteOnExit();
			
			// Write "1" for every appearance of the word
			PrintWriter writer = new PrintWriter(new FileWriter(file, true));
			writer.println(k + "," + newKey.get(k));
			writer.close();
        }

        // Write result back
        String BucketName = "ckreduceresults";
        for(String fileId : fileList){
	        File file = new File(fileId);
	        RecordHandler.WriteResult(BucketName, fileId, file);
        }

        // RecordHandler.displayAll(BucketName, "wordcount");
	}
	
	/**
	 * reduce of WordCount
	 * takes key and value, adds up count for every word occurrence
	 *  
	 * @param key		word
	 * @param value		count number
	 */
	private void reduce(String key, String value) {
		
		if(newKey.containsKey(key)) {
			int val = newKey.get(key) + Integer.parseInt(value);
			newKey.put(key, val);
		}
		else {
			int val = Integer.parseInt(value);
			newKey.put(key, val);
		}
	}
}

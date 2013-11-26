
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Hashtable;

/**
 * WordCount Reducer
 * @author hsong
 *
 */
public class WordCountReduce {

	protected String bucketName;
	protected String[] splits;
	protected Hashtable<String, Integer> newKey;
	
	/**
	 * WordCountReduce Constructor
	 * @param bucketName		bucket name
	 * @param splits			splits name
	 * @throws Exception 
	 */
	public WordCountReduce(String bucketName, String[] splits) throws IOException {
		this.bucketName = bucketName;
		this.splits = splits;
		this.newKey = new Hashtable<String,Integer>();
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
		
		for(String split:splits) {
			// Read every split
			InputStream input = RecordHandler.LoadSplit(bucketName, split);
			BufferedReader reader = new BufferedReader(new InputStreamReader(input));
			
	        while (true) {
	            String line = reader.readLine();
	            if (line == null) break;
	            
	            // Parse line
	            String[] tmp = line.split(",");
	            String key = tmp[0];
	            String value = tmp[1];
	            
	            // Start reduce
	            reduce(key, value);
	        }
	        reader.close();
		}
		
        for(String k:newKey.keySet()) {
        	System.out.println(k + " " + newKey.get(k));
        }
        /*
        // Write result back
        for(String surfix : newKey.keys()){
	        newKey = "wordcount_map_" + Split + "_" + surfix;
	        
	        File file = new File(surfix);
	        RecordHandler.WriteResult(BucketName, newKey, file);
        }
        */
        // Display all objects on S3 with prefix "wordcount"
        // System.out.println();
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

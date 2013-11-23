import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

/**
 * WordCount mapreduce
 * probably make it as a package in the future
 * 
 * @author hsong
 *
 */
public class WordCount {

}

/**
 * Mapper of WordCount
 * first load the split data, just as RecordReader
 * then pass <key, value> pair to map method
 * each Mapper handle a single split
 * 
 * @author hsong
 *
 */
class Mapper extends Thread{
	
	protected String BucketName;
	protected String Split;				// Key for split
	protected String newKey;			// New key for emit
	protected Set<String> fileList;		// Word list for generating new keys
	
	/**
	 * Mapper Constructor
	 * @param bucketName		bucket name for the split
	 * @param Split				split name as the key
	 */
	public Mapper(String bucketName, String Split){
		this.BucketName = bucketName;
		this.Split = Split;
		this.fileList = new HashSet<String>();
	}
	
	/**
	 * Main method of word count map
	 * 
	 * loads data stream from S3 and counts the word, 
	 * then emit count result to separated files for each word
	 * write files back to S3  
	 */
	public void run(){
		try{
			InputStream input = RecordHandler.LoadSplit(BucketName, Split);
	        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
	        
	        while (true) {
	            String line = reader.readLine();
	            if (line == null) break;

	            map(Split, line);
	        }
	        reader.close();
	        
	        
	        // Write result back
	        for(String surfix : fileList){
		        newKey = "wordcount_map_" + surfix;
		        
		        File file = new File(surfix);
		        RecordHandler.WriteResult(BucketName, newKey, file);
	        }
	        
	        // Display all objects on S3 with prefix "wordcount"
	        // System.out.println();
	        // RecordHandler.displayAll(BucketName, "wordcount");
	        
		} catch (Exception ex) {
			System.out.println(ex.getMessage());
		}
	}


	/**
	 * map of WordCount
	 * takes key and value, emits 1 for every word occurrence in value
	 * write results in file named as first 2 characters of the key  
	 * 
	 * @param key				document name
	 * @param value		 		document content
	 * @throws IOException 
	 */
	private void map(String key, String value) throws IOException {
		// presume words are separated by space  
		String[] words = value.split(" ");
		
		// Emit results to corresponding file
		for(String word: words) {
			
			// All key with same two first characters goto same file
			String fileId = word.substring(0, 2).toLowerCase();
			
			// Check if it is a word
			if(!Character.isLetter(fileId.charAt(0)) | !Character.isLetter(fileId.charAt(0))) {
				continue;
			}
			
			fileId += "_" + Split;
			// Add word as a key to Emit list
			fileList.add(fileId);
			
			// Append the value to emit file
			File file = new File(fileId);
			//file.deleteOnExit();
			
			// Write "1" for every appearance of the word
			PrintWriter writer = new PrintWriter(new FileWriter(file, true));
			writer.println(word + "," + 1);
			writer.close();
		}
	}
}


/**
 * Reducer for WordCount
 * 
 * @author hsong
 *
 */
class Reducer extends Thread{
	
	protected String bucketName;
	protected String[] splits;
	protected Hashtable<String, Integer> newKey;
	
	/**
	 * Reducer Constructor
	 * @param bucketName		bucket name
	 * @param splits			splits name
	 */
	public Reducer(String bucketName, String[] splits) {
		this.bucketName = bucketName;
		this.splits = splits;
		this.newKey = new Hashtable<String,Integer>();
	}
	
	/**
	 * Main method of reduce count reduce
	 * 
	 * loads data stream from S3 and counts the word, 
	 * then emit count result to separated files for each word
	 * write files back to S3  
	 */
	public void run() {
		try {
			
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
			
		} catch (Exception ex) {
			// TODO: handle exception
			System.out.println(ex.getCause());
		}
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
import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Set;

public class WordCount {

}

class Map extends Thread{
	
	protected String BucketName;
	protected String Key;				// Key for split
	protected String newKey;			// New key for emit
	protected Set<String> wordList;		// Word list for generate new keys
	
	public Map(String bucketName, String Key){
		this.BucketName = bucketName;
		this.Key = Key;
		this.wordList = new HashSet<String>();
		
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
			InputStream input = RecordHandler.LoadSplit(BucketName, Key);
	        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
	        
	        while (true) {
	            String line = reader.readLine();
	            if (line == null) break;

	            map(Key, line);
	        }
	        
	        /*
	        // Write result back
	        for(String surfix : EmitList){
		        String newKey = "wordcount_map_" + Key + "_" + surfix;
		        
		        File file = new File(surfix);
		        RecordHandler.WriteResult(BucketName, newKey, file);
	        }*/
	        
	        // Display all objects on S3 with prefix "wordcount"
	        //System.out.println();
	        //RecordHandler.displayAll(BucketName, "wordcount");
	        
		} catch (Exception ex) {
			System.out.println(ex.getMessage());
		}
	}

	/**
	 * map of WordCount
	 * 
	 * @param key
	 * 		document name
	 * @param value
	 * 		document content
	 * @throws IOException 
	 */
	private void map(String key, String value) throws IOException {
		// key: document name
		// value: document content
		String[] words = value.split(" ");
		
		// Emit results to corresponding file
		for(String word: words) {
			
			// Add word as a key to Emit list
			wordList.add(word);
			
			// Append the value to emit file
			File file = new File(word);
			file.deleteOnExit();
			
			// Write "1" for every appearance of the word
			PrintWriter writer = new PrintWriter(new FileWriter(file, true));
			writer.println(1);
			writer.close();
		}
	}
}

class Reduce extends Thread{
	/**
	 * Main method of reduce count map
	 * 
	 * loads data stream from S3 and counts the word, 
	 * then emit count result to separated files for each word
	 * write files back to S3  
	 */
	public void run() {
		
	}
}
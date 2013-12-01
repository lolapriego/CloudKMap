
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Mapper of WordCount
 * first load the split data, just as RecordReader
 * then pass <key, value> pair to map method
 * each Mapper handle a single split
 * 
 * @author hsong
 *
 */
public class WordCountMap {
	
	protected String BucketName;
	protected String Split;				// Key for split
	protected Hashtable<String, Integer> Keys;			// Key table for combiner
	protected Set<String> KeyId;						// New key for reduce
	protected Set<String> fileList;		// File list for generating new keys
	protected ArrayList<String> buffer; // Buffer for loading s3 object
	
	protected static String resultBucket = "ckmapresults";
	
	/**
	 * WordCountMap Constructor
	 * @param bucketName		bucket name for the split
	 * @param Split				split name as the key
	 * @throws Exception 
	 */
	public WordCountMap(String bucketName, String Split, ArrayList<String> buffer) throws IOException{
		this.BucketName = bucketName;
		this.Split = Split;
		this.Keys = new Hashtable<String, Integer>();
		this.KeyId = new HashSet<String>();
		this.fileList = new HashSet<String>();
		this.buffer = buffer;
		run();
	}
	
	/**
	 * Main method of word count map
	 * 
	 * loads data stream from S3 and counts the word, 
	 * then emit count result to separated files for each word
	 * write files back to S3  
	 */
	public void run() throws IOException{

        // Do map
        for(String line:buffer) {
        	map(Split, line);
        }
        
		// Write combined value for # of appearance of the word
		for(String k:Keys.keySet()) {

			// All key with same two first characters goto same file
			String keyId = k.substring(0,1).toUpperCase();
			String fileId = keyId + "_" + Split;
			
			// Add word as a key to Emit list
			KeyId.add(keyId);
			fileList.add(fileId);

			// Append the value to emit file
			File file = new File(fileId);
			file.deleteOnExit();

			PrintWriter writer = new PrintWriter(new FileWriter(file, true));
			writer.println(k + "," + Keys.get(k));
			writer.close();
		}
        
        // Write result back
        for(String filename : fileList){
	        File file = new File(filename);
	        RecordHandler.WriteResult(resultBucket, filename, file);
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
		// Search words using regex
		Pattern pattern = Pattern.compile("[a-zA-Z]+");
		Matcher matcher = pattern.matcher(value);
		
		while (matcher.find()) {

			// Find the word matched the pattern
            String word = matcher.group();
            if(word.length() < 2) continue;

			// Count key
			if(Keys.containsKey(word))
				Keys.put(word, Keys.get(word) + 1);
			else
				Keys.put(word, 1);
		}
	}
	
	
	/**
	 * Get map result list
	 * 
	 * @return	result file list separated by commas
	 */
	public String getFileList() {
		String outString="";
		for(String str: fileList) {
			outString += str + ",";
		}
		return outString.substring(0, outString.length()-1);
	}
	
	/**
	 * Get Keys for map results
	 * @return
	 */
	public String getKeyId() {
		String outString="";
		for(String str: KeyId) {
			outString += str + ",";
		}
		return outString.substring(0, outString.length()-1);
	}
}



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

import javax.sound.sampled.Line;

import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;


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
	protected Hashtable<String, Integer> Keys;			// New key for emit
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

			String fileId = k + "_" + Split.substring(Split.length()-5, Split.length());
			
			// Add word as a key to Emit list
			fileList.add(fileId);

			// Append the value to emit file
			File file = new File(fileId);
			file.deleteOnExit();

			PrintWriter writer = new PrintWriter(new FileWriter(file, true));
			writer.println(k + "," + Keys.get(k));
			writer.close();
		}
        
        // Write result back
        for(String newkey : fileList){
	        File file = new File(newkey);
	        RecordHandler.WriteResult(resultBucket, newkey, file);
        }
        
        // Display all objects on S3 with prefix "wordcount"
        // System.out.println();
        // RecordHandler.displayAll(BucketName, "wordcount");
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
			if(!Character.isLetter(fileId.charAt(0)) | !Character.isLetter(fileId.charAt(1))) {
				continue;
			}
			
			// Add key
			if(Keys.containsKey(fileId))
				Keys.put(fileId, Keys.get(fileId) + 1);
			else
				Keys.put(fileId, 1);
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
	public String getKeys() {
		String outString="";
		for(String str: Keys.keySet()) {
			outString += str + ",";
		}
		return outString.substring(0, outString.length()-1);
	}
}


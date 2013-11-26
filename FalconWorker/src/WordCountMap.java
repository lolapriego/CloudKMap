
import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Set;


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
	protected String newKey;			// New key for emit
	protected Set<String> fileList;		// File list for generating new keys
	
	protected static String resultBucket = "ckmapresults";
	
	/**
	 * WordCountMap Constructor
	 * @param bucketName		bucket name for the split
	 * @param Split				split name as the key
	 * @throws Exception 
	 */
	public WordCountMap(String bucketName, String Split) throws IOException{
		this.BucketName = bucketName;
		this.Split = Split;
		this.fileList = new HashSet<String>();
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

		InputStream input = RecordHandler.LoadSplit(BucketName, Split);
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
	        
        while (true) {
            String line = reader.readLine();
            if (line == null) break;

            map(Split, line);
        }
        reader.close();
        
        
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
			
			fileId = fileId + "_" + Split.charAt(Split.length()-1);
			
			// Add word as a key to Emit list
			fileList.add(fileId);
			
			// Append the value to emit file
			File file = new File(fileId);
			file.deleteOnExit();
			
			// Write "1" for every appearance of the word
			PrintWriter writer = new PrintWriter(new FileWriter(file, true));
			writer.println(word + "," + 1);
			writer.close();
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
}


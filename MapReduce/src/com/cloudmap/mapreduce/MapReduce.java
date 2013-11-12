
package com.cloudmap.mapreduce;

/**
 * MapReduce for Worker
 * so far just defined main class that takes bucket name and splits name
 * need adding Task Message Parser
 * 
 * @author hsong
 *
 */
public class MapReduce {
	public static void main(String args[]) throws Exception {
		new InputFormat("mapreduce-words-count-0", "words0");
		//new WordCountReduce("mapreduce-words-count-0", "wordcount_map_ma_words0,wordcount_map_ma_words1".split(","));
	}
}

/**
 * Input Format
 * takes in bucket name and splits name
 * and pass to each Mapper for reading
 * 
 * @author hsong
 *
 */
class InputFormat {
	
	protected String bucketName;
	protected String split;
	
	/**
	 * InputFormat
	 * @param bucketName	bucket name
	 * @param splits		splits name, separated by ','
	 */
	public InputFormat(String bucketName, String split) {
		this.bucketName = bucketName;
		this.split = split;
		start();
	}
	
	/**
	 * Start creating Mapper to load each split
	 */
	private void start() {
		try {
			
			// Start mapper
			WordCountMap mapper = new WordCountMap(bucketName, split);
			
			/**
			 *  When every mapper is done,
			 *  should return complete message back to client
			 */
			
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
}

import java.util.ArrayList;

/**
 * MapReduce for Worker
 * so far just defined main class that takes bucket name and splits name
 * need adding Task Message Parser
 * 
 * @author hsong
 *
 */
public class MapReduce {
	public static void main(String args[]) {
		new InputFormat("mapreduce-words-count-0", "words0,words1");
		new Reducer("mapreduce-words-count-0", "wordcount_map_ma_words0,wordcount_map_ma_words1".split(",")).start();
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
	protected String[] splits;
	
	/**
	 * InputFormat
	 * @param bucketName	bucket name
	 * @param splits		splits name, separated by ','
	 */
	public InputFormat(String bucketName, String splits) {
		this.bucketName = bucketName;
		this.splits = splits.split(",");
		start();
	}
	
	/**
	 * Start creating Mapper to load each split
	 */
	private void start() {
		try {
			// Create a thread for every split
			// keep track of all threads
			ArrayList<Mapper> threads = new ArrayList<Mapper>(); 
			
			// Start threads
			for(String Key : splits) {
				Mapper mapper = new Mapper(bucketName, Key);
				threads.add(mapper);
				mapper.start();
			}
			
			// Wait for all threads completed
			boolean lock = true;
			while(lock) {
				lock = false;
				
				for(Thread t: threads) {
					if(t.isAlive()) lock = true;
				}
			}
			
			/**
			 *  When every thread is done,
			 *  should return complete message back to client
			 */
			for(Mapper mapper: threads) {
				String str = mapper.newKey;
				System.out.println(str);
			}
			
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
}

import java.util.ArrayList;

public class MapReduce {
	public static void main(String args[]) {
		new StartMap("mapreduce-words-count-0", "words0");
	}
}

class StartMap {
	
	protected String bucketName;
	protected String[] splits;
	
	public StartMap(String bucketName, String splits) {
		this.bucketName = bucketName;
		this.splits = splits.split(",");
		start();
	}
	
	private void start() {
		try {
			// Create a thread for every split
			// keep track of all threads
			ArrayList<Map> threads = new ArrayList<Map>(); 
			
			// Start threads
			for(String Key : splits) {
				Map map = new Map(bucketName, Key);
				threads.add(map);
				map.start();
			}
			
			// Wait for all threads completed
			boolean lock = true;
			while(lock) {
				lock = false;
				
				for(Thread t: threads) {
					if(t.isAlive()) lock = true;
				}
			}
			
			// When every thread is done,
			// should return complete message back to client 
			for(Map m: threads) {
				String str = m.newKey;
				System.out.println(str);
			}
			
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
}

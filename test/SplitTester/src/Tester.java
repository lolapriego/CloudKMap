import java.util.List;


public class Tester {

	public static void main (String [] args){
		Splitter spliter = new Splitter("test.txt");
		List<String> paths = spliter.inputSplitter();
		
		for(String s: paths){
			System.out.println("PATH  " + paths);
		}
	}
}

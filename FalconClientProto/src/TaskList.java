import java.util.concurrent.ConcurrentHashMap;



class TaskList{
	
      public  static ConcurrentHashMap<Long, Task.Builder> tasks = new ConcurrentHashMap<Long,Task.Builder>();
      private static TaskList instance = null;
      private TaskList() {}
      public static TaskList getInstance(){
    	  if (instance == null)
    		  instance = new TaskList();
    	  return instance;
      }

      
}
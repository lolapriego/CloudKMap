import java.util.concurrent.ConcurrentHashMap;

import com.cloudmap.message.TaskMessage.Task;



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
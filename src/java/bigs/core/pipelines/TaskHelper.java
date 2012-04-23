package bigs.core.pipelines;

import bigs.api.exceptions.BIGSException;
import bigs.api.storage.Put;
import bigs.api.storage.Result;

public class TaskHelper {
	public static Task fromResultObject(Result result, String columnFamily, String classColumn, String objectColumn) {
		Task r;
		byte[] taskClassNameBytes = result.getValue(columnFamily, classColumn);
		if (taskClassNameBytes!=null) {
			Object obj = null;
			try {
				obj = Class.forName(new String(taskClassNameBytes)).newInstance();
			} catch (Exception e) {
				throw new BIGSException("error getting task in schedule item "+result.getRowKey()+". "+e.getMessage());
			}
			if (! (obj instanceof Task)) {
				throw new BIGSException("task schedule item in db with rowkey '"+result.getRowKey()+"' must instantiate "+Task.class.getName());
			}
			
			r = (Task)obj;
			byte[] taskObject = result.getValue(columnFamily, objectColumn);
			if (taskObject!=null) {
				r.fromTextRepresentation(new String(taskObject));
			}
			return r;
		} else {
			return null;
		}
	}
	
	public static void toPutObject(Task task, Put put, String columnFamily, String classColumn, String objectColumn) {
		put.add(columnFamily, classColumn, task.getClass().getName());
		put.add(columnFamily, objectColumn, task.toTextRepresentation());							
	}
}

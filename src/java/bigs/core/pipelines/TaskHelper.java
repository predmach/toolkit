package bigs.core.pipelines;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import bigs.api.core.BIGSException;
import bigs.api.core.BIGSParam;
import bigs.api.core.Task;
import bigs.api.storage.Put;
import bigs.api.storage.Result;
import bigs.core.utils.Text;

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
	
    /**
     * returns a description of this algorithm together with the description of the
     * fields marked as MLLExplorationParam
     * @return a list of Strings with the lines of the help text
     */
    public static List<String> getHelp(Task task) {
            List<String> r = new ArrayList<String>();
            
            r.add(Text.leftJustify(task.getClass().getName(),60)+"  "+task.getDescription());
            for (Field field: task.getClass().getFields()) {                        
                    if (field.isAnnotationPresent(BIGSParam.class)) {
                            
                            Annotation annotation = field.getAnnotation(BIGSParam.class);
                            String description = "";
                            if (annotation!=null) {
                                    try {
                                            description = (String)annotation.annotationType().getMethod("description").invoke(annotation);
                                    } catch (Exception e) {
                                            
                                    }
                            }
                            StringBuffer sb = new StringBuffer();
                            sb.append(Text.rightJustify(field.getName(),40));
                            sb.append(" ").append(Text.leftJustify(field.getType().getSimpleName(), 20));
                            sb.append(" ").append(description);
                            r.add(sb.toString());
                    }
            }
            return r;
    }
        
    public static void toPutObject(Task task, Put put, String columnFamily, String classColumn, String objectColumn) {
		put.add(columnFamily, classColumn, task.getClass().getName());
		put.add(columnFamily, objectColumn, task.toTextRepresentation());							
	}
}

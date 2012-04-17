package pilot.core;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import bigs.api.core.BIGSParam;
import bigs.api.core.Configurable;
import bigs.api.exceptions.BIGSException;
import bigs.core.exceptions.BIGSPropertyNotFoundException;
import bigs.core.utils.Core;
import bigs.core.utils.Log;
import bigs.core.utils.Text;

public abstract class TaskContainer implements Configurable, TextSerializable {

	List<TaskContainer> taskContainers = new ArrayList<TaskContainer>();

	TaskContainer parentTaskContainer = null;
	
	PipelineStage pipelineStage = null;
		
	public abstract List<Class<? extends TaskContainer>> allowedTaskContainers();		
	
	public abstract List<Class<? extends Task>> allowedTasks();
		
	public abstract List<TaskContainer> generateMyTaskContainers();	
	
	public abstract Boolean supportsParallelization();
	
	public abstract List<String> getDataItemTags(DataItem tag);

	
	public abstract TextSerializable processPreSubContainers(Task configuredTask, TextSerializable previousState);
	
	public abstract TextSerializable processPostSubContainers(Task configuredTask, TextSerializable previousState);
		
	
	public abstract TextSerializable processPreLoop(Task configuredTask, TextSerializable previousState);
	
	public abstract TextSerializable processPostLoop(Task configuredTask, List<TextSerializable> previousStates);	

	/**
	 * returs void because pre-process-post all run within the same process and, thus, 
	 * only post needs to return a state for the framework to handle
	 * @param previousState
	 */
	public abstract void processPreDataBlock(Task configuredTask, TextSerializable previousState);
	
	public abstract DataItem processDataItem(Task configuredTask, DataItem dataItem);

	public abstract TextSerializable processPostDataBlock(Task configuredTask);
			
	
	public List<TaskContainer> getTaskContainers() {
		return taskContainers;
	}
		
	public void addTaskContainer(TaskContainer taskContainer) {
		taskContainers.add(taskContainer);
		taskContainer.setParentTaskContainer(this);
		taskContainer.setPipelineStage(this.getPipelineStage());
	}

	public TaskContainer getParentTaskContainer() {
		return parentTaskContainer;
	}

	public void setParentTaskContainer(TaskContainer parentTaskContainer) {
		this.parentTaskContainer = parentTaskContainer;
	}

	public PipelineStage getPipelineStage() {
		return pipelineStage;
	}

	public void setPipelineStage(PipelineStage pipelineStage) {
		this.pipelineStage = pipelineStage;
	}
		
	public void printOut(String prefix) {

		System.out.println(prefix+this.toString());
		if (!this.taskContainers.isEmpty()) {
			for (TaskContainer tb: this.taskContainers) {
				tb.printOut(prefix+"     ");
			}
		} 
	}

	
	/**
	 * returns true if this task container can contain the task 
	 * passed as parameter
	 * @param task the task to check for
	 * @return true if this container accepts the task passed as parameter
	 */
	public Boolean allowsTask (Task task) {
		if (this.allowedTasks()==null) {
			throw new BIGSException("task container "+this.getClass().getName()+" does not support any task. check its implementation");
		}
		for (Class<? extends Task> c: this.allowedTasks()) {
			if (c.isAssignableFrom(task.getClass())) return true;			
		}
		return false;
	}
	
	/**
	 * Returns the last schedule item of the list.
	 * 
	 * @param prefix
	 * @param schedule
	 * @param parentScheduleItem
	 * @return
	 */
	public ScheduleItem fillSchedule(Schedule schedule, ScheduleItem parentScheduleItem) {
		if (!this.taskContainers.isEmpty()) {
			ScheduleItem p1 = new ScheduleItem(schedule, this, pipelineStage.configuredTask, "preSubContainers");
			if (parentScheduleItem!=null) p1.addParentId(parentScheduleItem.getId());

			TaskContainer th = this.taskContainers.get(0).clone();
			ScheduleItem p2 = new ScheduleItem(schedule, th, pipelineStage.configuredTask,  "preLoop").addParentId(p1.getId());

			List<Integer> subContainerParentsIds = new ArrayList<Integer>();
			if (this.taskContainers.size()>0) {
				Boolean isParallel = th.supportsParallelization();
				ScheduleItem parent = p2;
				for (TaskContainer tb: this.taskContainers) {
					ScheduleItem item = tb.fillSchedule(schedule, parent);
					if (isParallel) {
						subContainerParentsIds.add(item.getId());
					} else {
						if (tb == this.taskContainers.get(this.taskContainers.size()-1)) {
							subContainerParentsIds.add(item.getId());
						}
						parent = item;
					}				
				}
			}
				
			ScheduleItem p3 = new ScheduleItem(schedule, th	, pipelineStage.configuredTask, "postLoop").addParentsIds(subContainerParentsIds);

			ScheduleItem p4 = new ScheduleItem(schedule, this, pipelineStage.configuredTask, "postSubContainers").addParentId(p3.getId());
			return p4;
		} else {
			ScheduleItem p = new ScheduleItem(schedule, this, pipelineStage.configuredTask, "LOOP processDataItem").addParentId(parentScheduleItem.getId());
			return p;
		}
	}
	
	public Boolean allowsTaskContainer(Class<? extends TaskContainer> subContainerClass) {
		if (this.allowedTaskContainers()==null) {
			return false;
		} else {
			Boolean classAllowed = false;
			for (Class<? extends TaskContainer> c: this.allowedTaskContainers()) {
				if (c.isAssignableFrom(subContainerClass)) {
					classAllowed = true;
					break;
				}
			}
			return classAllowed;
		}		
	}

	public static List<TaskContainer> fromProperties(Properties properties, String propertiesPrefix, Integer containerNumber) {
		List<TaskContainer> r = new ArrayList<TaskContainer>();
		String containerPropertyName = "container."+Text.zeroPad(new Long(containerNumber), 2);
		try {
			TaskContainer container = Core.getConfiguredObject(containerPropertyName, TaskContainer.class, properties, propertiesPrefix);
			for (TaskContainer tc: container.generateMyTaskContainers()) {
				r.add(tc);
				List<TaskContainer> subContainers = TaskContainer.fromProperties(properties, propertiesPrefix, containerNumber+1);				
				if (subContainers!=null) {
					for (TaskContainer stc: subContainers) {
						if (tc.allowsTaskContainer(stc.getClass())) {
							tc.addTaskContainer(stc);
						} else {
							throw new BIGSException(tc.getClass().getSimpleName()+" does not allow task containers of type "+stc.getClass().getSimpleName());
						}
					}
				}
			}
			return r;
		} catch (BIGSPropertyNotFoundException e) {
			return null;
		}			
	}

	public TaskContainer clone() {
		Class<? extends TaskContainer> thisClass = this.getClass();
		TaskContainer r;
		try {
			r = (TaskContainer)thisClass.newInstance();
			for (Field field: thisClass.getFields()) {
				if (field.isAnnotationPresent(BIGSParam.class)) {
					field.set(r, field.get(this));
				}
			}
			return r;
		} catch (InstantiationException e) {
			throw new BIGSException("InstantiationExpception cloning TaskContainer. "+e.getMessage());
		} catch (IllegalAccessException e) {
			throw new BIGSException("IllegaAccesExpception cloning TaskContainer. "+e.getMessage());
		}
	}

}

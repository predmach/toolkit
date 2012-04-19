package pilot.core;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import pilot.core.data.LLDDataItem;
import pilot.testing.GenericDataType;

import bigs.api.core.BIGSParam;
import bigs.api.core.Configurable;
import bigs.api.exceptions.BIGSException;
import bigs.core.exceptions.BIGSPropertyNotFoundException;
import bigs.core.utils.Core;
import bigs.core.utils.Log;
import bigs.core.utils.Text;

public abstract class TaskContainer<T extends Task> implements Configurable, TextSerializable {

	List<TaskContainer<Task>> taskContainers = new ArrayList<TaskContainer<Task>>();

	TaskContainer<? extends Task> parentTaskContainer = null;
	
	PipelineStage pipelineStage = null;
			
	public abstract List<TaskContainer<T>> generateMyConfiguredTaskContainers();	
	
	public abstract Boolean supportsParallelization();
	
	public abstract List<String> getDataItemTags(LLDDataItem tag);

	
	public abstract TextSerializable processPreSubContainers(T configuredTask, TextSerializable previousState);
	
	public abstract TextSerializable processPostSubContainers(T configuredTask, TextSerializable previousState);
		
	
	public abstract TextSerializable processPreLoop(T configuredTask, TextSerializable previousState);
	
	public abstract TextSerializable processPostLoop(T configuredTask, List<TextSerializable> previousStates);	

	/**
	 * returs void because pre-process-post all run within the same process and, thus, 
	 * only post needs to return a state for the framework to handle
	 * @param previousState
	 */
	public abstract void processPreDataBlock(T configuredTask, TextSerializable previousState);
	
	public abstract LLDDataItem processDataItem(T configuredTask, LLDDataItem dataItem);

	public abstract TextSerializable processPostDataBlock(T configuredTask);
			
	
	public List<TaskContainer<Task>> getTaskContainers() {
		return taskContainers;
	}
		
	public void addTaskContainer(TaskContainer<Task> taskContainer) {
		taskContainers.add(taskContainer);
		taskContainer.setParentTaskContainer(this);
		taskContainer.setPipelineStage(this.getPipelineStage());
	}

	public TaskContainer<? extends Task> getParentTaskContainer() {
		return parentTaskContainer;
	}

	public void setParentTaskContainer(TaskContainer<? extends Task> parentTaskContainer) {
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
			for (TaskContainer<Task> tb: this.taskContainers) {
				tb.printOut(prefix+"     ");
			}
		} 
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

			TaskContainer<Task> th = this.taskContainers.get(0).clone();
			ScheduleItem p2 = new ScheduleItem(schedule, th, pipelineStage.configuredTask,  "preLoop").addParentId(p1.getId());

			List<Integer> subContainerParentsIds = new ArrayList<Integer>();
			if (this.taskContainers.size()>0) {
				Boolean isParallel = th.supportsParallelization();
				ScheduleItem parent = p2;
				for (TaskContainer<Task> tb: this.taskContainers) {
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
	

	public static List<TaskContainer<Task>> fromProperties(Properties properties, String propertiesPrefix, Integer containerNumber) {
		List<TaskContainer<Task>> r = new ArrayList<TaskContainer<Task>>();
		String containerPropertyName = "container."+Text.zeroPad(new Long(containerNumber), 2);
		try {
			TaskContainer<Task> container = Core.getConfiguredObject(containerPropertyName, TaskContainer.class, properties, propertiesPrefix);
			for (TaskContainer<Task> tc: container.generateMyConfiguredTaskContainers()) {
				r.add(tc);
				List<TaskContainer<Task>> subContainers = TaskContainer.fromProperties(properties, propertiesPrefix, containerNumber+1);				
				if (subContainers!=null) {
					for (TaskContainer<Task> stc: subContainers) {
							tc.addTaskContainer(stc);
					}
				}
			}
			return r;
		} catch (BIGSPropertyNotFoundException e) {
			return null;
		}			
	}

	public TaskContainer<T> clone() {
		Class<? extends TaskContainer> thisClass = this.getClass();
		TaskContainer<T> r;
		try {
			r = (TaskContainer<T>)thisClass.newInstance();
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

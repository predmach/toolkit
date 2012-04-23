package bigs.core.pipelines;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


import bigs.api.core.BIGSParam;
import bigs.api.core.Configurable;
import bigs.api.data.DataItem;
import bigs.api.exceptions.BIGSException;
import bigs.api.storage.Put;
import bigs.api.storage.Result;
import bigs.core.exceptions.BIGSPropertyNotFoundException;
import bigs.core.utils.Core;
import bigs.core.utils.Text;

public abstract class TaskContainer<T extends Task> implements Configurable, TextSerializable {

	List<TaskContainer<? extends Task>> taskContainers = new ArrayList<TaskContainer<? extends Task>>();

	TaskContainer<? extends Task> parentTaskContainer = null;
	
	PipelineStage pipelineStage = null;
			
	public abstract List<TaskContainer<T>> generateMyPreparedTaskContainers();	
	
	public abstract Boolean supportsParallelization();
	
	public abstract Map<String, String> getDataItemTags(String dataItemRowkey);

	public abstract Map<String, String> getMyTagsAsPrepared();
	
	public abstract State processPreSubContainers(T preparedTask, State previousState);
	
	public abstract State processPostSubContainers(T preparedTask, State previousState);
	
	public abstract <D extends DataItem> Boolean acceptsEmptyDataItem(T preparedTask, D dataItem);
	
	public abstract State processPreLoop(T preparedTask, State previousState);
	
	public abstract State processPostLoop(T preparedTask, List<State> previousStates);	

	/**
	 * returs void because pre-process-post all run within the same process and, thus, 
	 * only post needs to return a state for the framework to handle
	 * @param previousState
	 */
	public abstract void processPreDataBlock(T preparedTask, State previousState);
	
	public abstract <I extends DataItem> DataItem processDataItem(T preparedTask, I dataItem);

	public abstract State processPostDataBlock(T preparedTask);
				
	public List<TaskContainer<? extends Task>> getTaskContainers() {
		return taskContainers;
	}
	
		
	public void addTaskContainer(TaskContainer<? extends Task> taskContainer) {
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
			for (TaskContainer<? extends Task> tb: this.taskContainers) {
				tb.printOut(prefix+"     ");
			}
		} 
	}

	String getFullyQualifiedName() {
		String className   = this.getClass().getSimpleName();

		String stageNumber = "NONE";
		String pipelineNumber = "NONE";
		if (this.pipelineStage!=null) {
			stageNumber = Text.zeroPad(new Long(this.pipelineStage.stageNumber),5);
			if (this.pipelineStage.pipeline!=null) {
				pipelineNumber = Text.zeroPad(new Long(this.pipelineStage.pipeline.getPipelineNumber()), 5);
			}
		}
		
		return pipelineNumber+"."+stageNumber+"|"+className;
		
		
	}
	
	/**
	 * returns the fully qualified tags for a data item. This is,
	 * including the container class name, stage and pipeline number
	 * @return
	 */
	public Map<String, String> getFQNDataItemTags(String dataItemRowkey) {
		return this.appendFQNToTags(this.getDataItemTags(dataItemRowkey));
	}
	
	/**
	 * appends the fully qualified name to all tags in the argument
	 * @return a new Map with the same elements whose key has been appended the FQN
	 */
	public Map<String, String> appendFQNToTags(Map<String, String> tags) {
		if (tags==null) return null;
		String FQN = this.getFullyQualifiedName();
		Map<String, String> r = new HashMap<String, String>();
		for (String k: tags.keySet()) {
			r.put(FQN + "|"+k, tags.get(k));
		}
		return r;
	}
	
	/**
	 * returns the fully qualified tags for that this prepared container would expect
	 * @return
	 */
	public Map<String, String> getFQNMyTagsAsPrepared() {
		return this.appendFQNToTags(this.getMyTagsAsPrepared());
	}
	
	/**
	 * Recursively fills the schedule with the prepared subcontainers of this task container
	 * 
	 * @param prefix
	 * @param schedule
	 * @param parentScheduleItem
	 * @return the last schedule item inserted in the schedule 
	 */
	public ScheduleItem fillSchedule(Schedule schedule, ScheduleItem parentScheduleItem, Map<String, String> tags ) {
		
		// clones and joins received tags with the ones of this
		Map<String, String> myTags = this.getFQNMyTagsAsPrepared();
		Map<String, String> clonedTags = new HashMap<String, String>();
		if (tags!=null)   for (String k: tags.keySet())   clonedTags.put(k, tags.get(k));
		if (myTags!=null) for (String k: myTags.keySet()) clonedTags.put(k, myTags.get(k));
		
		// recurses over the existing subcontainers
		if (!this.taskContainers.isEmpty()) {
			ScheduleItem p1 = new ScheduleItem(schedule, this, pipelineStage.preparedTask, ScheduleItem.METHOD_PRESUBCONTAINERS);
			if (parentScheduleItem!=null) p1.addParentId(parentScheduleItem.getId());

			TaskContainer<? extends Task> th = this.taskContainers.get(0).clone();
			ScheduleItem p2 = new ScheduleItem(schedule, th, pipelineStage.preparedTask,  ScheduleItem.METHOD_PRELOOP).addParentId(p1.getId());

			List<Integer> subContainerParentsIds = new ArrayList<Integer>();
			if (this.taskContainers.size()>0) {
				Boolean isParallel = th.supportsParallelization();
				ScheduleItem parent = p2;
				for (TaskContainer<? extends Task> tb: this.taskContainers) {
					ScheduleItem item = tb.fillSchedule(schedule, parent, clonedTags);
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
				
			ScheduleItem p3 = new ScheduleItem(schedule, th	, pipelineStage.preparedTask, ScheduleItem.METHOD_POSTLOOP).addParentsIds(subContainerParentsIds);
			
			ScheduleItem p4 = new ScheduleItem(schedule, this, pipelineStage.preparedTask, ScheduleItem.METHOD_POSTSUBCONTAINERS).addParentId(p3.getId());
			return p4;
		} else {
			ScheduleItem p = new ScheduleItem(schedule, this, pipelineStage.preparedTask, ScheduleItem.METHOD_LOOPDATA).addParentId(parentScheduleItem.getId());
			p.setTags(clonedTags);
			return p;
		}
	}
	

	public static List<TaskContainer<Task>> fromProperties(Properties properties, String propertiesPrefix, Integer containerNumber) {
		List<TaskContainer<Task>> r = new ArrayList<TaskContainer<Task>>();
		String containerPropertyName = "container."+Text.zeroPad(new Long(containerNumber), 2);
		try {
			TaskContainer<Task> container = Core.getPreparedObject(containerPropertyName, TaskContainer.class, properties, propertiesPrefix);
			for (TaskContainer<Task> tc: container.generateMyPreparedTaskContainers()) {
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
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static TaskContainer<Task> fromResultObject(Result result, String columnFamily, String classColumn, String objectColumn) {
		TaskContainer r;
		byte[] taskContainerClassNameBytes = result.getValue(columnFamily, classColumn);
		if (taskContainerClassNameBytes!=null) {
			Object obj = null;
			try {
				obj = Class.forName(new String(taskContainerClassNameBytes)).newInstance();				
			} catch (Exception e) {
				throw new BIGSException("error getting task container in schedule item "+result.getRowKey()+". "+e.getMessage());
			}
			if (! (obj instanceof TaskContainer)) {
				throw new BIGSException("task container schedule item in db with rowkey '"+result.getRowKey()+"' must instantiate "+TaskContainer.class.getName());
			}
			
			r = (TaskContainer)obj;
			byte[] taskContainerObject = result.getValue(columnFamily, objectColumn);
			if (taskContainerObject!=null) {
				r.fromTextRepresentation(new String(taskContainerObject));
			}
			return r;
		} else {
			return null;
		}
	}
	
	public void toPutObject (Put put, String columnFamily, String classColumn, String objectColumn) {
		put.add(columnFamily, classColumn, this.getClass().getName());
		put.add(columnFamily, objectColumn, this.toTextRepresentation());					
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
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

package pilot.modules.containers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import bigs.api.core.BIGSParam;
import bigs.api.exceptions.BIGSException;

import pilot.core.DataItem;
import pilot.core.Task;
import pilot.core.TaskContainer;
import pilot.core.TextSerializable;

public class DataPartitionTaskContainer extends TaskContainer {

	@BIGSParam
	public Integer numberOfPartitions = 1;
	
	@BIGSParam
	public Integer partitionNumber = null;
	
	public DataPartitionTaskContainer() {		
	}
	
	public DataPartitionTaskContainer(Integer numberOfPartitions) {
		this.numberOfPartitions = numberOfPartitions;
	}
	
	public DataPartitionTaskContainer(Integer numberOfPartitions, Integer partitionNumber) {
		this.numberOfPartitions = numberOfPartitions;
		this.partitionNumber = partitionNumber;
	}
	
	public Integer getNumberOfPartitions() {
		return this.numberOfPartitions;
	}


	@Override
	public List<TaskContainer> generateMyTaskContainers() {
		List<TaskContainer> r = new ArrayList<TaskContainer>();
		for (int i=1; i<= this.numberOfPartitions; i++) {
			TaskContainer tb = new DataPartitionTaskContainer(this.numberOfPartitions, i);
			r.add(tb); 
		}
		return r;		
	}

	@Override
	public List<Class<? extends TaskContainer>> allowedTaskContainers() {
		List<Class<? extends TaskContainer>> r = new ArrayList<Class<? extends TaskContainer>>();
		r.add(TaskContainer.class);
		return r;
	}

	@Override
	public List<Class<? extends Task>> allowedTasks() {
		 List<Class<? extends Task>> r = new  ArrayList<Class<? extends Task>>();
		 r.add(DataPartitionTask.class);
		 return r;
	}

	@Override
	public Boolean supportsParallelization() {
		return true;
	}


	@Override
	public String toString() {
		return "DataPartitionTaskContainer [numberOfPartitions="
				+ numberOfPartitions + ", partitionNumber=" + partitionNumber
				+ "]";
	}

	@Override
	public void processPreDataBlock(Task configuredTask, TextSerializable previousState) {
		DataPartitionTask task = (DataPartitionTask)configuredTask;
		task.startPartition(previousState);		
	}

	@Override
	public DataItem processDataItem(Task configuredTask, DataItem dataItem) {
		return null;
	}

	@Override
	public TextSerializable processPostDataBlock(Task configuredTask) {
		DataPartitionTask task = (DataPartitionTask)configuredTask;
		TextSerializable returningState = task.finalizePartition();
		return returningState;				
	}

	@Override
	public TextSerializable processPreSubContainers(Task configuredTask, TextSerializable previousState) {
		DataPartitionTask task = (DataPartitionTask)configuredTask;
		TextSerializable returningState = task.beforeProcessingPartitionSubContainers(previousState);
		return returningState;		
	}

	@Override
	public TextSerializable processPostSubContainers(Task configuredTask, TextSerializable previousState) {
		DataPartitionTask task = (DataPartitionTask)configuredTask;
		TextSerializable returningState = task.afterProcessingPartitionSubContainers(previousState);
		return returningState;		
	}

	@Override
	public TextSerializable processPreLoop(Task configuredTask, TextSerializable previousState) {
		DataPartitionTask task = (DataPartitionTask)configuredTask;
		TextSerializable returningState = task.beforeProcessingAllPartitions(previousState);
		return returningState;		
	}

	@Override
	public TextSerializable processPostLoop(Task configuredTask, List<TextSerializable> previousStates) {
		DataPartitionTask task = (DataPartitionTask)configuredTask;
		TextSerializable returningState = task.afterProcessingAllPartitions(previousStates);
		return returningState;
	}

	@Override
	public List<String> getDataItemTags(DataItem tag) {
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public String toTextRepresentation() {
		JSONObject obj = new JSONObject();
		obj.put("partitions", this.numberOfPartitions);
		obj.put("partitionNumber", this.partitionNumber);
		return obj.toString();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void fromTextRepresentation(String textRepresentation) {
		
		JSONParser parser = new JSONParser();
		try {
			Map<String, Long> json = (Map<String, Long>)parser.parse(textRepresentation);
			if (json.get("partitions")!=null) this.numberOfPartitions = json.get("partitions").intValue();
			if (json.get("partitionNumber")!=null) this.partitionNumber = json.get("partitionNumber").intValue();
			
		} catch (ParseException e) {
			throw new BIGSException("error parsing JSON representation of "+this.getClass().getName());
		}
	}
}

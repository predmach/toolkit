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
	public TextSerializable processPreSubContainers(TextSerializable previousState) {
		return null;
	}

	@Override
	public TextSerializable processPostSubContainers(TextSerializable previousState) {
		return null;
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
		return false;
	}

	@Override
	public void processPreDataBlock(TextSerializable previousState) {
		
	}

	@Override
	public DataItem processDataItem(DataItem dataItem) {
		return null;
	}

	@Override
	public TextSerializable processPostDataBlock() {
		return null;
		
	}

	@Override
	public String toString() {
		return "DataPartitionTaskContainer [numberOfPartitions="
				+ numberOfPartitions + ", partitionNumber=" + partitionNumber
				+ "]";
	}

	@Override
	public TextSerializable processPreLoop(TextSerializable previousState) {
		return null;
	}

	@Override
	public TextSerializable processPostLoop(List<TextSerializable> previousState) {
		return null;
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

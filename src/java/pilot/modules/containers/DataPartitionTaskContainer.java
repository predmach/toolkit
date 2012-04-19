package pilot.modules.containers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import bigs.api.core.BIGSParam;
import bigs.api.exceptions.BIGSException;

import pilot.core.TaskContainer;
import pilot.core.TextSerializable;
import pilot.core.data.LLDDataItem;

public class DataPartitionTaskContainer extends TaskContainer<DataPartitionTask> {

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
	public List<TaskContainer<DataPartitionTask>> generateMyConfiguredTaskContainers() {
		List<TaskContainer<DataPartitionTask>> r = new ArrayList<TaskContainer<DataPartitionTask>>();
		for (int i=1; i<= this.numberOfPartitions; i++) {
			TaskContainer<DataPartitionTask> tb = new DataPartitionTaskContainer(this.numberOfPartitions, i);
			r.add(tb); 
		}
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
	public void processPreDataBlock(DataPartitionTask configuredTask, TextSerializable previousState) {
		configuredTask.startPartition(previousState);		
	}

	@Override
	public LLDDataItem processDataItem(DataPartitionTask configuredTask, LLDDataItem dataItem) {
		return null;
	}

	@Override
	public TextSerializable processPostDataBlock(DataPartitionTask configuredTask) {
		TextSerializable returningState = configuredTask.finalizePartition();
		return returningState;				
	}

	@Override
	public TextSerializable processPreSubContainers(DataPartitionTask configuredTask, TextSerializable previousState) {
		TextSerializable returningState = configuredTask.beforeProcessingPartitionSubContainers(previousState);
		return returningState;		
	}

	@Override
	public TextSerializable processPostSubContainers(DataPartitionTask configuredTask, TextSerializable previousState) {
		DataPartitionTask task = (DataPartitionTask)configuredTask;
		TextSerializable returningState = task.afterProcessingPartitionSubContainers(previousState);
		return returningState;		
	}

	@Override
	public TextSerializable processPreLoop(DataPartitionTask configuredTask, TextSerializable previousState) {
		TextSerializable returningState = configuredTask.beforeProcessingAllPartitions(previousState);
		return returningState;		
	}

	@Override
	public TextSerializable processPostLoop(DataPartitionTask configuredTask, List<TextSerializable> previousStates) {
		TextSerializable returningState = configuredTask.afterProcessingAllPartitions(previousStates);
		return returningState;
	}

	@Override
	public List<String> getDataItemTags(LLDDataItem tag) {
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

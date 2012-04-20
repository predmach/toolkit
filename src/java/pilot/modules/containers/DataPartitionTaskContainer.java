package pilot.modules.containers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import bigs.api.core.BIGSParam;
import bigs.api.exceptions.BIGSException;

import pilot.core.State;
import pilot.core.TaskContainer;
import pilot.core.data.DataItem;
import pilot.core.data.LLDDataItem;

public class DataPartitionTaskContainer extends TaskContainer<DataPartitionTask<State,DataItem,DataItem>> {

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
	public List<TaskContainer<DataPartitionTask<State,DataItem,DataItem>>> generateMyConfiguredTaskContainers() {
		List<TaskContainer<DataPartitionTask<State,DataItem,DataItem>>> r = new ArrayList<TaskContainer<DataPartitionTask<State,DataItem,DataItem>>>();
		for (int i=1; i<= this.numberOfPartitions; i++) {
			TaskContainer<DataPartitionTask<State,DataItem,DataItem>> tb = new DataPartitionTaskContainer(this.numberOfPartitions, i);
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
	public void processPreDataBlock(DataPartitionTask<State,DataItem,DataItem> configuredTask, State previousState) {
		configuredTask.startPartition(previousState);		
	}

	@Override
	public <D extends DataItem> D processDataItem(DataPartitionTask<State,DataItem,DataItem> configuredTask, D dataItem) {
		
		configuredTask.processPartitionDataItem(dataItem);
		
		return null;
	}

	@Override
	public State processPostDataBlock(DataPartitionTask<State,DataItem,DataItem> configuredTask) {
		State returningState = configuredTask.finalizePartition();
		return returningState;				
	}

	@Override
	public State processPreSubContainers(DataPartitionTask<State,DataItem,DataItem> configuredTask, State previousState) {
		State returningState = configuredTask.beforeProcessingPartitionSubContainers(previousState);
		return returningState;		
	}

	@Override
	public State processPostSubContainers(DataPartitionTask<State,DataItem,DataItem> configuredTask, State previousState) {
		DataPartitionTask<State,DataItem,DataItem> task = (DataPartitionTask<State,DataItem,DataItem>)configuredTask;
		State returningState = task.afterProcessingPartitionSubContainers(previousState);
		return returningState;		
	}

	@Override
	public State processPreLoop(DataPartitionTask<State,DataItem,DataItem> configuredTask, State previousState) {
		State returningState = configuredTask.beforeProcessingAllPartitions(previousState);
		return returningState;		
	}

	@Override
	public State processPostLoop(DataPartitionTask<State,DataItem,DataItem> configuredTask, List<State> previousStates) {
		State returningState = configuredTask.afterProcessingAllPartitions(previousStates);
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

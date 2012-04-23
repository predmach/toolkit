package bigs.modules.containers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import bigs.api.core.BIGSParam;
import bigs.api.data.DataItem;
import bigs.api.exceptions.BIGSException;
import bigs.core.pipelines.State;
import bigs.core.pipelines.TaskContainer;


public class DataPartitionTaskContainer extends TaskContainer<DataPartitionTask<State, DataItem, DataItem>> {

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
	public List<TaskContainer<DataPartitionTask<State,DataItem,DataItem>>> generateMyPreparedTaskContainers() {
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
	public <I extends DataItem> DataItem processDataItem(DataPartitionTask<State,DataItem,DataItem> configuredTask, I dataItem) {
		DataItem o = configuredTask.processPartitionDataItem(dataItem);
		return o;
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
	public Map<String, String> getDataItemTags(String dataItemRowkey) {
		
		Double d = Math.random()*this.numberOfPartitions+1;
		Map<String, String> r = new HashMap<String, String>();
		
		r.put("partition", new Integer(d.intValue()).toString());
		return r;
	}

	@Override
	public Map<String, String> getMyTagsAsPrepared() {
		if (this.partitionNumber==null) return null;
		
		Map<String, String> r = new HashMap<String, String>();
		r.put("partition", this.partitionNumber.toString());
		return r;
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

	@Override
	public <D extends DataItem> Boolean acceptsEmptyDataItem(DataPartitionTask<State,DataItem,DataItem> configuredTask, D dataItem) {
		return configuredTask.acceptsEmptyDataItemForPartition(dataItem);
	}
}

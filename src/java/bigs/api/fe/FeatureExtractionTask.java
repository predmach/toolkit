package bigs.api.fe;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import bigs.api.core.BIGSException;
import bigs.api.core.BIGSParam;
import bigs.api.core.State;
import bigs.api.core.Task;
import bigs.api.core.TaskContainer;
import bigs.api.data.RawDataItem;
import bigs.api.data.LLDDataItem;
import bigs.api.tasks.DataPartitionTask;
import bigs.modules.containers.DataPartitionTaskContainer;

/**
 * Feature extraction algorithms take an array of bytes (typically an image) and return a vector
 * or a matrix, represented as a List of Lists of Doubles ... in case of a vectors the top level list
 * contains one single List of Doubles. Note that this allows for matrices to have rows of 
 * different lengths.
 * 
 * @author rlx
 *
 */
public abstract class FeatureExtractionTask implements DataPartitionTask<State, RawDataItem, LLDDataItem> {
	
	
	@BIGSParam(description="number of splits into which the input dataset is partitioned")
	public Integer numberOfPartitions = 1;

	public Integer partitionNumber = 1;
	
	/**
	 * this method must be implemented by specific feature extraction algorithms
	 * @param source
	 * @return
	 */
	public abstract List<List<Double>> extractFeatures (byte[] source);

	@Override
	public List<TaskContainer<? extends Task>> getTaskContainerCascade() {
		List<TaskContainer<? extends Task>> r = new ArrayList<TaskContainer<? extends Task>>();
		r.add(new DataPartitionTaskContainer(numberOfPartitions));
		
		return r;
	}
	
	@Override
	public String getDescription() {
		return "";
	}

	@SuppressWarnings("unchecked")
	@Override
	public String toTextRepresentation() {	 
		JSONObject obj = new JSONObject();
		obj.put("partitions", this.numberOfPartitions);
		return obj.toString();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void fromTextRepresentation(String textRepresentation) {
		
		JSONParser parser = new JSONParser();
		try {
			Map<String, Long> json = (Map<String, Long>)parser.parse(textRepresentation);
			if (json.get("partitions")!=null) this.numberOfPartitions = json.get("partitions").intValue();
			
		} catch (ParseException e) {
			throw new BIGSException("error parsing JSON representation of "+this.getClass().getName());
		}
	}
	

	@Override
	public State beforeProcessingPartitionSubContainers(State previousState) {
		return null;
	}

	@Override
	public State afterProcessingPartitionSubContainers(State previousState) {
		return null;
	}

	@Override
	public void startPartition(State previousState) {
	}

	@Override
	public LLDDataItem processPartitionDataItem(RawDataItem item) {
		return new LLDDataItem(this.extractFeatures(item.getBytes()));
	}

	@Override
	public State finalizePartition() {
		return null;
	}

	@Override
	public State beforeProcessingAllPartitions(State previousState) {
		return null;
	}

	@Override
	public State afterProcessingAllPartitions(List<State> previousStates) {
		return null;
	}

	@Override
	public Boolean acceptsEmptyDataItemForPartition(RawDataItem dataItem) {
		return true;
	}
	
}

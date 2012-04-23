package bigs.api.examples.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import bigs.api.core.BIGSParam;
import bigs.api.data.DataItem;
import bigs.api.data.LLDDataItem;
import bigs.api.exceptions.BIGSException;
import bigs.core.pipelines.Task;
import bigs.core.pipelines.TaskContainer;
import bigs.core.utils.Log;
import bigs.modules.containers.DataPartitionTask;
import bigs.modules.containers.DataPartitionTaskContainer;
import bigs.modules.containers.IterativeTask;
import bigs.modules.containers.IterativeTaskContainer;

public class IterateAndSplit implements DataPartitionTask<IterateAndSplitState, LLDDataItem, DataItem>, IterativeTask<IterateAndSplitState, DataItem, DataItem> {

	@BIGSParam
	public Integer numberOfIterations;
	
	@BIGSParam
	public Integer numberOfPartitions;
	
	@SuppressWarnings("unchecked")
	@Override
	public String toTextRepresentation() {
		JSONObject obj = new JSONObject();
		obj.put("iterations", this.numberOfIterations);
		obj.put("partitions", this.numberOfPartitions);
		return obj.toString();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void fromTextRepresentation(String textRepresentation) {
		
		JSONParser parser = new JSONParser();
		try {
			Map<String, Long> json = (Map<String, Long>)parser.parse(textRepresentation);
			if (json.get("iterations")!=null) this.numberOfIterations = json.get("iterations").intValue();
			if (json.get("partitions")!=null) this.numberOfPartitions = json.get("partitions").intValue();
			
		} catch (ParseException e) {
			throw new BIGSException("error parsing JSON representation of "+this.getClass().getName());
		}
	}
	
	@Override
	public String toString() {
		return "KMeans [numberOfIterations=" + numberOfIterations
				+ ", numberOfPartitions=" + numberOfPartitions + "]";
	}

	@Override
	public List<TaskContainer<? extends Task>> getTaskContainerCascade() {
		List<TaskContainer<? extends Task>> r = new ArrayList<TaskContainer<? extends Task>>();
		r.add(new IterativeTaskContainer(numberOfIterations));
		r.add(new DataPartitionTaskContainer(numberOfPartitions));
		return r;
	}	

	/** -------------------------------------------------------------------

	    Methods for IterativeTaskContainer
	    
	    -------------------------------------------------------------------*/

	@Override
	public IterateAndSplitState startIteration(IterateAndSplitState previousState) {	
		previousState.value = previousState.value +1D;
		return previousState;
	}


	@Override
	public IterateAndSplitState finalizeIteration(IterateAndSplitState previousState) {
		previousState.value = previousState.value/5.0D;
		return previousState;
	}


	@Override
	public IterateAndSplitState beforeAllIterations(IterateAndSplitState previousState) {
		if (previousState == null) {
			previousState = new IterateAndSplitState();
		}
		previousState.value = 1D;
		return previousState;
	}

	@Override
	public IterateAndSplitState afterAllIterations(List<IterateAndSplitState> previousStates) {
		
		if (previousStates.size()!=1) {
			throw new BIGSException("expecting only one iteration state in sequential process");
		}
		
		IterateAndSplitState k = previousStates.get(0);
		k.value = k.value/6D;
		Log.info("---> FINAL STATE "+k.toTextRepresentation());
		return k;
	}


	
	@Override
	public void startDataBlock(IterateAndSplitState previousState) {}

	@Override
	public DataItem processIterativeDataItem(DataItem item) { return null; }

	@Override
	public IterateAndSplitState finalizeDataBlock() { return null; }


	/** -------------------------------------------------------------------

    	Methods for DataPartitionTaskContainer
  
      	-------------------------------------------------------------------*/

	IterateAndSplitState partitionState = new IterateAndSplitState();
	@Override
	public void startPartition(IterateAndSplitState previousState) {
		partitionState.value = previousState.value*3;
	}


	@Override
	public IterateAndSplitState finalizePartition() {
		partitionState.value = partitionState.value*4;
		return partitionState;
	}


	@Override
	public IterateAndSplitState beforeProcessingAllPartitions(IterateAndSplitState previousState) {
		previousState.value = previousState.value + 2;
		return previousState;
	}


	@Override
	public IterateAndSplitState afterProcessingAllPartitions(List<IterateAndSplitState> previousStates) {
		IterateAndSplitState s = new IterateAndSplitState();
		s.value=0D;
		for (IterateAndSplitState p: previousStates) {
			s.value = s.value + p.value;
		}
		return s;
	}

	@Override
	public IterateAndSplitState beforeProcessingPartitionSubContainers(IterateAndSplitState previousState) {
		return previousState;
	}


	@Override
	public IterateAndSplitState afterProcessingPartitionSubContainers(IterateAndSplitState previousState) {
		return previousState;
	}

	@Override
	public DataItem processPartitionDataItem(LLDDataItem item) {
		// TODO Auto-generated method stub
		return item;
	}

	@Override
	public Boolean acceptsEmptyDataItemForIterative(DataItem dataItem) {
		return true;
	}

	@Override
	public Boolean acceptsEmptyDataItemForPartition(LLDDataItem dataItem) {
		return true;
	}



}

package bigs.api.examples.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import bigs.api.core.BIGSException;
import bigs.api.core.BIGSParam;
import bigs.api.core.Task;
import bigs.api.core.TaskContainer;
import bigs.api.data.DataItem;
import bigs.api.data.LLDDataItem;
import bigs.api.tasks.DataPartitionTask;
import bigs.api.tasks.IterativeTask;
import bigs.core.utils.Log;
import bigs.modules.containers.DataPartitionTaskContainer;
import bigs.modules.containers.IterativeTaskContainer;

public class IterateAndSplit implements DataPartitionTask<StateWithDoubleValue, LLDDataItem, LLDDataItem>, IterativeTask<StateWithDoubleValue, DataItem, DataItem> {

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
	public StateWithDoubleValue startIteration(StateWithDoubleValue previousState) {	
		previousState.value = previousState.value +1D;
		return previousState;
	}


	@Override
	public StateWithDoubleValue finalizeIteration(StateWithDoubleValue previousState) {
		previousState.value = previousState.value/5.0D;
		return previousState;
	}


	@Override
	public StateWithDoubleValue beforeAllIterations(StateWithDoubleValue previousState) {
		if (previousState == null) {
			previousState = new StateWithDoubleValue();
		}
		previousState.value = 1D;
		return previousState;
	}

	@Override
	public StateWithDoubleValue afterAllIterations(List<StateWithDoubleValue> previousStates) {
		
		if (previousStates.size()!=1) {
			throw new BIGSException("expecting only one iteration state in sequential process");
		}
		
		StateWithDoubleValue k = previousStates.get(0);
		k.value = k.value/6D;
		Log.info("---> FINAL STATE "+k.toTextRepresentation());
		return k;
	}


	
	@Override
	public void startDataBlock(StateWithDoubleValue previousState) {}

	@Override
	public DataItem processIterativeDataItem(DataItem item) { return null; }

	@Override
	public StateWithDoubleValue finalizeDataBlock() { return null; }


	/** -------------------------------------------------------------------

    	Methods for DataPartitionTaskContainer
  
      	-------------------------------------------------------------------*/

	StateWithDoubleValue partitionState = new StateWithDoubleValue();
	@Override
	public void startPartition(StateWithDoubleValue previousState) {
		partitionState.value = previousState.value*3;
	}


	@Override
	public StateWithDoubleValue finalizePartition() {
		partitionState.value = partitionState.value*4;
		return partitionState;
	}


	@Override
	public StateWithDoubleValue beforeProcessingAllPartitions(StateWithDoubleValue previousState) {
		previousState.value = previousState.value + 2;
		return previousState;
	}


	@Override
	public StateWithDoubleValue afterProcessingAllPartitions(List<StateWithDoubleValue> previousStates) {
		StateWithDoubleValue s = new StateWithDoubleValue();
		s.value=0D;
		for (StateWithDoubleValue p: previousStates) {
			s.value = s.value + p.value;
		}
		return s;
	}

	@Override
	public StateWithDoubleValue beforeProcessingPartitionSubContainers(StateWithDoubleValue previousState) {
		return previousState;
	}


	@Override
	public StateWithDoubleValue afterProcessingPartitionSubContainers(StateWithDoubleValue previousState) {
		return previousState;
	}

	@Override
	public LLDDataItem processPartitionDataItem(LLDDataItem item) {
		Double accum = 0D;
		for (List<Double> l: item.getLLD()) {
			for (Double d: l) {
				accum = accum + d;
			}
		}
		
		List<Double> rl = new ArrayList<Double>();
		rl.add(accum);
		LLDDataItem r = new LLDDataItem();
		r.addLD(rl);		
		return r;
	}

	@Override
	public Boolean acceptsEmptyDataItemForIterative(DataItem dataItem) {
		return true;
	}

	@Override
	public Boolean acceptsEmptyDataItemForPartition(LLDDataItem dataItem) {
		return true;
	}

	@Override
	public String getDescription() {
		return "sample iterate and split task";
	}



}

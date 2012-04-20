package pilot.core.examples.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import bigs.api.core.BIGSParam;
import bigs.api.exceptions.BIGSException;
import bigs.core.utils.Log;
import pilot.core.Task;
import pilot.core.TaskContainer;
import pilot.core.data.DataItem;
import pilot.core.data.LLDDataItem;
import pilot.modules.containers.DataPartitionTask;
import pilot.modules.containers.DataPartitionTaskContainer;
import pilot.modules.containers.IterativeTask;
import pilot.modules.containers.IterativeTaskContainer;

public class KMeans implements DataPartitionTask<KMeansState, LLDDataItem, LLDDataItem>, IterativeTask<KMeansState, DataItem, DataItem> {

	@BIGSParam
	public Integer numberOfCentroids;
	
	@BIGSParam
	public Integer numberOfIterations;
	
	@BIGSParam
	public Integer numberOfPartitions;
	
	@SuppressWarnings("unchecked")
	@Override
	public String toTextRepresentation() {
		JSONObject obj = new JSONObject();
		obj.put("centroids", this.numberOfCentroids);
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
			if (json.get("centroids")!=null) this.numberOfCentroids = json.get("centroids").intValue();
			if (json.get("iterations")!=null) this.numberOfIterations = json.get("iterations").intValue();
			if (json.get("partitions")!=null) this.numberOfPartitions = json.get("partitions").intValue();
			
		} catch (ParseException e) {
			throw new BIGSException("error parsing JSON representation of "+this.getClass().getName());
		}
	}

	@Override
	public String toString() {
		return "KMeans [numberOfCentroids=" + numberOfCentroids + "]";
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
	public KMeansState startIteration(KMeansState previousState) {	
		previousState.value = previousState.value +1D;
		return previousState;
	}


	@Override
	public KMeansState finalizeIteration(KMeansState previousState) {
		previousState.value = previousState.value/5.0D;
		return previousState;
	}


	@Override
	public KMeansState beforeAllIterations(KMeansState previousState) {
		previousState.value = 1D;
		return previousState;
	}

	@Override
	public KMeansState afterAllIterations(List<KMeansState> previousStates) {
		
		if (previousStates.size()!=1) {
			throw new BIGSException("expecting only one iteration state in sequential process");
		}
		
		KMeansState k = previousStates.get(0);
		k.value = k.value/6D;
		Log.info("---> FINAL STATE "+k.toTextRepresentation());
		return k;
	}


	
	@Override
	public void startDataBlock(KMeansState previousState) {}

	@Override
	public DataItem processIterativeDataItem(DataItem item) { return null; }

	@Override
	public KMeansState finalizeDataBlock() { return null; }


	/** -------------------------------------------------------------------

    	Methods for DataPartitionTaskContainer
  
      	-------------------------------------------------------------------*/

	KMeansState partitionState = new KMeansState();
	@Override
	public void startPartition(KMeansState previousState) {
		partitionState.value = previousState.value*3;
	}


	@Override
	public KMeansState finalizePartition() {
		partitionState.value = partitionState.value*4;
		return partitionState;
	}


	@Override
	public KMeansState beforeProcessingAllPartitions(KMeansState previousState) {
		previousState.value = previousState.value + 2;
		return previousState;
	}


	@Override
	public KMeansState afterProcessingAllPartitions(List<KMeansState> previousStates) {
		KMeansState s = new KMeansState();
		s.value=0D;
		for (KMeansState p: previousStates) {
			s.value = s.value + p.value;
		}
		return s;
	}

	@Override
	public KMeansState beforeProcessingPartitionSubContainers(KMeansState previousState) {
		return null;
	}


	@Override
	public KMeansState afterProcessingPartitionSubContainers(KMeansState previousState) {
		return null;
	}

	@Override
	public LLDDataItem processPartitionDataItem(LLDDataItem item) {
		// TODO Auto-generated method stub
		return null;
	}


}

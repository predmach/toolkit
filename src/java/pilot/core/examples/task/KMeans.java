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
import pilot.core.TextSerializable;
import pilot.core.data.LLDDataItem;
import pilot.modules.containers.DataPartitionTask;
import pilot.modules.containers.DataPartitionTaskContainer;
import pilot.modules.containers.IterativeTask;
import pilot.modules.containers.IterativeTaskContainer;

public class KMeans implements DataPartitionTask, IterativeTask {

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
	public List<TaskContainer<Task>> getTaskContainerCascade() {
		List<TaskContainer<Task>> r = new ArrayList<TaskContainer<Task>>();
		r.add(new IterativeTaskContainer(numberOfIterations));
		r.add(new DataPartitionTaskContainer(numberOfPartitions));
		
		return r;
	}

	
	/** -------------------------------------------------------------------

	      Methods for IterativeTaskContainer
	    
	    -------------------------------------------------------------------*/

	@Override
	public TextSerializable startIteration(TextSerializable previousState) {
		Log.debug(this.getClass().getName()+" "+Thread.currentThread().getStackTrace()[1].getMethodName());
		
		KMeansState s = (KMeansState)previousState;
		s.value = s.value +1D;

		Log.debug(s.toTextRepresentation());
		return s;
	}


	@Override
	public TextSerializable finalizeIteration(TextSerializable previousState) {
		Log.debug(this.getClass().getName()+" "+Thread.currentThread().getStackTrace()[1].getMethodName());
		KMeansState s = (KMeansState)previousState;
		s.value = s.value/5.0D;
		return s;
	}


	@Override
	public TextSerializable beforeAllIterations(TextSerializable previousState) {
		Log.debug(this.getClass().getName()+" "+Thread.currentThread().getStackTrace()[1].getMethodName());
		
		KMeansState s = new KMeansState();
		s.value = 1D;
		return s;
	}

	@Override
	public TextSerializable afterAllIterations(
			List<TextSerializable> previousStates) {
		
		if (previousStates.size()!=1) {
			throw new BIGSException("expecting only one iteration state in sequential process");
		}
		
		KMeansState k = (KMeansState)previousStates.get(0);
		k.value = k.value/6D;
		Log.info("---> FINAL STATE "+k.toTextRepresentation());
		return k;
	}


	@Override
	public void startDataBlock(TextSerializable previousState) {
		Log.debug(this.getClass().getName()+" "+Thread.currentThread().getStackTrace()[1].getMethodName());
	}


	@Override
	public TextSerializable finalizeDataBlock() {
		Log.debug(this.getClass().getName()+" "+Thread.currentThread().getStackTrace()[1].getMethodName());
		return null;
	}


	/** -------------------------------------------------------------------

    	Methods for DataPartitionTaskContainer
  
      	-------------------------------------------------------------------*/

	KMeansState partitionState = new KMeansState();
	@Override
	public void startPartition(TextSerializable previousState) {
		Log.debug(this.getClass().getName()+" "+Thread.currentThread().getStackTrace()[1].getMethodName());		
		partitionState.value = ((KMeansState)previousState).value*3;
	}


	@Override
	public TextSerializable finalizePartition() {
		Log.debug(this.getClass().getName()+" "+Thread.currentThread().getStackTrace()[1].getMethodName());
		partitionState.value = partitionState.value*4;
		return partitionState;
	}


	@Override
	public TextSerializable beforeProcessingAllPartitions(
			TextSerializable previousState) {
		Log.debug(this.getClass().getName()+" "+Thread.currentThread().getStackTrace()[1].getMethodName());
		KMeansState s = (KMeansState)previousState;
		s.value = s.value + 2;
		return s;
	}


	@Override
	public TextSerializable afterProcessingAllPartitions(
			List<TextSerializable> previousStates) {
		Log.debug(this.getClass().getName()+" "+Thread.currentThread().getStackTrace()[1].getMethodName());
		Log.debug(this.getClass().getName()+" "+Thread.currentThread().getStackTrace()[1].getMethodName());
		KMeansState s = new KMeansState();
		s.value=0D;
		for (TextSerializable p: previousStates) {
			s.value = s.value + ((KMeansState)p).value;
		}
		return s;
	}

	@Override
	public TextSerializable beforeProcessingPartitionSubContainers(
			TextSerializable previousState) {
		Log.debug(this.getClass().getName()+" "+Thread.currentThread().getStackTrace()[1].getMethodName());
		return null;
	}


	@Override
	public TextSerializable afterProcessingPartitionSubContainers(
			TextSerializable previousState) {
		Log.debug(this.getClass().getName()+" "+Thread.currentThread().getStackTrace()[1].getMethodName());
		return null;
	}

	@Override
	public LLDDataItem processDataItem(LLDDataItem item) {
		// TODO Auto-generated method stub
		return null;
	}

	
}

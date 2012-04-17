package pilot.modules.ml.kmeans;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import bigs.api.core.BIGSParam;
import bigs.api.exceptions.BIGSException;
import pilot.core.DataItem;
import pilot.core.TaskContainer;
import pilot.core.TextSerializable;
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
	
	@Override
	public DataItem processDataItem(DataItem item) {
		// TODO Auto-generated method stub
		return null;
	}


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
	public List<TaskContainer> getTaskContainerCascade() {
		List<TaskContainer> r = new ArrayList<TaskContainer>();
		r.add(new IterativeTaskContainer(numberOfIterations));
		r.add(new DataPartitionTaskContainer(numberOfPartitions));
		return r;
	}



	@Override
	public TextSerializable beforeProcessingPartition(
			TextSerializable previousState) {
		// TODO Auto-generated method stub
		return null;
	}



	@Override
	public TextSerializable afterProcessingPartition(
			TextSerializable previousState) {
		// TODO Auto-generated method stub
		return null;
	}



	@Override
	public void startPartition(TextSerializable previousState) {
		// TODO Auto-generated method stub
		
	}



	@Override
	public TextSerializable finalizePartition() {
		// TODO Auto-generated method stub
		return null;
	}



	@Override
	public TextSerializable beforeProcessingAllPartitions(
			TextSerializable previousState) {
		// TODO Auto-generated method stub
		return null;
	}



	@Override
	public TextSerializable afterProcessingAllPartitions(
			List<TextSerializable> previousStates) {

		KMeansDataPartitionState ks = new KMeansDataPartitionState();
		ks.processedPartitions = previousStates.size();
		return ks;
	}



	@Override
	public TextSerializable beforeIteration(TextSerializable previousState) {
		return new KMeansIterationState();
	}



	@Override
	public TextSerializable afterIteration(TextSerializable previousState) {
		KMeansIterationState s = new KMeansIterationState();
		s.dummyParameter = 2;
		return s;
	}



	@Override
	public TextSerializable beforeAllIterations() {
		// TODO Auto-generated method stub
		return null;
	}



	@Override
	public TextSerializable afterAllIterations(
			List<TextSerializable> previousStates) {
		// TODO Auto-generated method stub
		return null;
	}	
	
}

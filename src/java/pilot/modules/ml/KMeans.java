package pilot.modules.ml;

import java.util.ArrayList;
import java.util.List;

import bigs.api.core.BIGSParam;
import pilot.core.DataItem;
import pilot.core.TaskContainer;
import pilot.core.TextSerializable;
import pilot.modules.containers.DataPartitionTask;
import pilot.modules.containers.DataPartitionTaskContainer;
import pilot.modules.containers.IterativeTaskContainer;

public class KMeans implements DataPartitionTask {

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

	@Override
	public void beforeProcessingPartition() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void afterProcessingPartition() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String toTextRepresentation() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TextSerializable fromTextRepresentation(String textRepresentation) {
		// TODO Auto-generated method stub
		return null;
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
	
}

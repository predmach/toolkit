package pilot.modules.fe;

import java.util.ArrayList;
import java.util.List;

import bigs.api.core.BIGSParam;

import pilot.core.DataItem;
import pilot.core.TaskContainer;
import pilot.core.TextSerializable;
import pilot.modules.containers.DataPartitionTask;
import pilot.modules.containers.DataPartitionTaskContainer;

public class SampleFeatureExtractor implements DataPartitionTask {

	@BIGSParam
	public Integer numberOfSplits = 1;
	
	@Override
	public void beforeProcessingPartition() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void afterProcessingPartition() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public DataItem processDataItem(DataItem item) {
		// TODO Auto-generated method stub
		return null;
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
	public List<TaskContainer> getTaskContainerCascade() {
		List<TaskContainer> r = new ArrayList<TaskContainer>();
		r.add(new DataPartitionTaskContainer(numberOfSplits));
		return r;
	}


}

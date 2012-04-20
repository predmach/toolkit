package pilot.modules.fe;

import java.util.ArrayList;
import java.util.List;

import bigs.api.core.BIGSParam;

import pilot.core.Task;
import pilot.core.TaskContainer;
import pilot.core.TextSerializable;
import pilot.core.data.LLDDataItem;
import pilot.core.data.RawDataItem;
import pilot.modules.containers.DataPartitionTask;
import pilot.modules.containers.DataPartitionTaskContainer;

public class SampleFeatureExtractor implements DataPartitionTask<RawDataItem, LLDDataItem> {

	@BIGSParam
	public Integer numberOfSplits = 1;
	

	@Override
	public String toTextRepresentation() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void fromTextRepresentation(String textRepresentation) {
	}

	@Override
	public List<TaskContainer<? extends Task>> getTaskContainerCascade() {
		List<TaskContainer<? extends Task>> r = new ArrayList<TaskContainer<? extends Task>>();
		r.add(new DataPartitionTaskContainer(numberOfSplits));
		
		return r;
	}

	@Override
	public TextSerializable beforeProcessingPartitionSubContainers(
			TextSerializable previousState) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TextSerializable afterProcessingPartitionSubContainers(
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public LLDDataItem processPartitionDataItem(RawDataItem item) {
		// TODO Auto-generated method stub
		return null;
	}


}

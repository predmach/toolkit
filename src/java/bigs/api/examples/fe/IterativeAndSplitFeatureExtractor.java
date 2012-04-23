package bigs.api.examples.fe;

import java.util.ArrayList;
import java.util.List;

import bigs.api.core.BIGSParam;
import bigs.api.data.LLDDataItem;
import bigs.api.data.RawDataItem;
import bigs.core.pipelines.State;
import bigs.core.pipelines.Task;
import bigs.core.pipelines.TaskContainer;
import bigs.modules.containers.DataPartitionTask;
import bigs.modules.containers.DataPartitionTaskContainer;


public class IterativeAndSplitFeatureExtractor implements DataPartitionTask<State, RawDataItem, LLDDataItem> {

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
	public State beforeProcessingPartitionSubContainers(
			State previousState) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public State afterProcessingPartitionSubContainers(
			State previousState) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void startPartition(State previousState) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public State finalizePartition() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public State beforeProcessingAllPartitions(
			State previousState) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public State afterProcessingAllPartitions(
			List<State> previousStates) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public LLDDataItem processPartitionDataItem(RawDataItem item) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Boolean acceptsEmptyDataItemForPartition(RawDataItem dataItem) {
		return true;
	}


}

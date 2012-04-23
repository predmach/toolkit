package bigs.modules.containers;

import java.util.List;

import bigs.api.data.DataItem;
import bigs.core.pipelines.State;
import bigs.core.pipelines.Task;


public interface DataPartitionTask<S extends State, I extends DataItem, O extends DataItem> extends Task {

	public S beforeProcessingPartitionSubContainers(S previousState);
	
	public S afterProcessingPartitionSubContainers(S previousState);
	
	public void startPartition(S previousState);
		
	public O processPartitionDataItem(I item);
	
	public S finalizePartition();
	
	public S beforeProcessingAllPartitions(S previousState);
	
	public S afterProcessingAllPartitions(List<S> previousStates);
	
	public Boolean acceptsEmptyDataItemForPartition(I dataItem);	
	
}

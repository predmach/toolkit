package pilot.modules.containers;

import java.util.List;

import pilot.core.State;
import pilot.core.Task;
import pilot.core.TextSerializable;
import pilot.core.data.DataItem;

public interface DataPartitionTask<S extends State, I extends DataItem, O extends DataItem> extends Task {

	public S beforeProcessingPartitionSubContainers(S previousState);
	
	public S afterProcessingPartitionSubContainers(S previousState);
	
	public void startPartition(S previousState);
		
	public O processPartitionDataItem(I item);
	
	public S finalizePartition();
	
	public S beforeProcessingAllPartitions(S previousState);
	
	public S afterProcessingAllPartitions(List<S> previousStates);
	
}

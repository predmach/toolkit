package pilot.modules.containers;

import java.util.List;

import pilot.core.DataItem;
import pilot.core.Task;
import pilot.core.TextSerializable;

public interface DataPartitionTask extends Task {

	public TextSerializable beforeProcessingPartitionSubContainers(TextSerializable previousState);
	
	public TextSerializable afterProcessingPartitionSubContainers(TextSerializable previousState);
	
	public void startPartition(TextSerializable previousState);
		
	public DataItem processDataItem(DataItem item);
	
	public TextSerializable finalizePartition();
	
	public TextSerializable beforeProcessingAllPartitions(TextSerializable previousState);
	
	public TextSerializable afterProcessingAllPartitions(List<TextSerializable> previousStates);
	
}

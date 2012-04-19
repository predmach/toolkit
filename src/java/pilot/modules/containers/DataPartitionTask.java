package pilot.modules.containers;

import java.util.List;

import pilot.core.Task;
import pilot.core.TextSerializable;
import pilot.core.data.LLDDataItem;

public interface DataPartitionTask extends Task {

	public TextSerializable beforeProcessingPartitionSubContainers(TextSerializable previousState);
	
	public TextSerializable afterProcessingPartitionSubContainers(TextSerializable previousState);
	
	public void startPartition(TextSerializable previousState);
		
	public LLDDataItem processDataItem(LLDDataItem item);
	
	public TextSerializable finalizePartition();
	
	public TextSerializable beforeProcessingAllPartitions(TextSerializable previousState);
	
	public TextSerializable afterProcessingAllPartitions(List<TextSerializable> previousStates);
	
}

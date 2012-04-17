package pilot.modules.containers;

import java.util.List;

import pilot.core.DataItem;
import pilot.core.Task;
import pilot.core.TextSerializable;

public interface DataPartitionTask extends Task {

	public TextSerializable beforeProcessingPartition(TextSerializable previousState);
	
	public TextSerializable afterProcessingPartition(TextSerializable previousState);
	
	public void startPartition(TextSerializable previousState);
		
	public DataItem processDataItem(DataItem item);
	
	public TextSerializable finalizePartition();
	
	public TextSerializable beforeProcessingAllPartitions(TextSerializable previousState);
	
	public TextSerializable afterProcessingAllPartitions(List<TextSerializable> previousStates);
	
}

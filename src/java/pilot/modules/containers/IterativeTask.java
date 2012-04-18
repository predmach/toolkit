package pilot.modules.containers;

import java.util.List;

import pilot.core.DataItem;
import pilot.core.Task;
import pilot.core.TextSerializable;

public interface IterativeTask extends Task {
	
	public TextSerializable startIteration(TextSerializable previousState);
	
	public TextSerializable finalizeIteration(TextSerializable previousState);
	
	public TextSerializable beforeAllIterations(TextSerializable previousState);
	
	public TextSerializable afterAllIterations(List<TextSerializable> previousStates);
	
	public void startDataBlock(TextSerializable previousState);
	
	public DataItem processDataItem(DataItem item);
	
	public TextSerializable finalizeDataBlock();
	
	
}

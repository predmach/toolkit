package bigs.api.tasks;

import java.util.List;

import bigs.api.core.State;
import bigs.api.core.Task;
import bigs.api.data.DataItem;


public interface IterativeTask<S extends State, I extends DataItem, O extends DataItem> extends Task {
	
	public S startIteration(S previousState);
	
	public S finalizeIteration(S previousState);
	
	public S beforeAllIterations(S previousState);
	
	public S afterAllIterations(List<S> previousStates);
	
	public void startDataBlock(S previousState);
	
	public O processIterativeDataItem(I item);
	
	public S finalizeDataBlock();
	
	public Boolean acceptsEmptyDataItemForIterative(I dataItem);	
}

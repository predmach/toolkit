package pilot.modules.containers;

import java.util.List;

import pilot.core.State;
import pilot.core.Task;
import pilot.core.data.DataItem;

public interface IterativeTask<S extends State, I extends DataItem, O extends DataItem> extends Task {
	
	public S startIteration(S previousState);
	
	public S finalizeIteration(S previousState);
	
	public S beforeAllIterations(S previousState);
	
	public S afterAllIterations(List<S> previousStates);
	
	public void startDataBlock(S previousState);
	
	public O processIterativeDataItem(I item);
	
	public S finalizeDataBlock();
	
	
}

package pilot.modules.containers;

import java.util.List;

import pilot.core.Task;
import pilot.core.TextSerializable;

public interface IterativeTask extends Task {
	
	public TextSerializable beforeIteration(TextSerializable previousState);
	
	public TextSerializable afterIteration(TextSerializable previousState);
	
	public TextSerializable beforeAllIterations();
	
	public TextSerializable afterAllIterations(List<TextSerializable> previousStates);
	

}

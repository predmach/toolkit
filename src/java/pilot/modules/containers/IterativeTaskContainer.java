package pilot.modules.containers;

import java.util.ArrayList;
import java.util.List;

import bigs.api.core.BIGSParam;
import pilot.core.DataItem;
import pilot.core.Task;
import pilot.core.TaskContainer;
import pilot.core.TextSerializable;

public class IterativeTaskContainer extends TaskContainer {

	public Integer numberOfIterations;
	
	Integer iterationNumber = null;
	
	public IterativeTaskContainer() {		
	}
	
	public IterativeTaskContainer(Integer numberOfIterations) {
		this.numberOfIterations = numberOfIterations;
	}
	
	public IterativeTaskContainer(Integer numberOfIterations, Integer iterationNumber) {
		this.numberOfIterations = numberOfIterations;
		this.iterationNumber = iterationNumber;
	}
	
	@Override
	public TextSerializable processPreSubContainers(TextSerializable previousState) {
		return null;
	}

	@Override
	public TextSerializable processPostSubContainers(TextSerializable previousState) {
		return null;
	}

	@Override
	public List<TaskContainer> generateMyTaskContainers() {
		List<TaskContainer> r = new ArrayList<TaskContainer>();
		for (int i=1; i<= this.numberOfIterations; i++) {
			TaskContainer tb = new IterativeTaskContainer(this.numberOfIterations, i);
			r.add(tb);
		}		
		return r;
		
	}

	@Override
	public List<Class<? extends TaskContainer>> allowedTaskContainers() {
		List<Class<? extends TaskContainer>> r = new ArrayList<Class<? extends TaskContainer>>();
		r.add(TaskContainer.class);
		return r;
	}

	@Override
	public List<Class<? extends Task>> allowedTasks() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Boolean supportsParallelization() {
		return false;
	}

	@Override
	public void processPreDataBlock(TextSerializable previousState) {
		
	}

	@Override
	public DataItem processDataItem(DataItem dataItem) {
		return null;
	}

	@Override
	public TextSerializable processPostDataBlock() {
		return null;
		
	}
	@Override
	public String toString() {
		return "IterativeTaskContainer [numberOfIterations=" + numberOfIterations
				+ ", iterationNumber=" + iterationNumber + "]";
	}

	@Override
	public TextSerializable processPreLoop(TextSerializable previousState) {
		return null;
	}

	@Override
	public TextSerializable processPostLoop(List<TextSerializable> previousState) {
		return null;
	}

	
	
	
}

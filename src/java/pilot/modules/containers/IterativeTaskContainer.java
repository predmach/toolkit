package pilot.modules.containers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import bigs.api.core.BIGSParam;
import bigs.api.exceptions.BIGSException;
import pilot.core.DataItem;
import pilot.core.Task;
import pilot.core.TaskContainer;
import pilot.core.TextSerializable;

public class IterativeTaskContainer extends TaskContainer {

	@BIGSParam
	public Integer numberOfIterations;
	
	@BIGSParam
	public Integer iterationNumber = null;
	
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
		 List<Class<? extends Task>> r = new  ArrayList<Class<? extends Task>>();
		 r.add(IterativeTask.class);
		 return r;
	}

	@Override
	public Boolean supportsParallelization() {
		return false;
	}

	@Override
	public TextSerializable processPreSubContainers(Task configuredTask, TextSerializable previousState) {
		IterativeTask myTask = (IterativeTask)configuredTask;
		TextSerializable resultingState = myTask.startIteration(previousState);
		return resultingState;
	}

	@Override
	public TextSerializable processPostSubContainers(Task configuredTask, TextSerializable previousState) {
		IterativeTask myTask = (IterativeTask)configuredTask;
		TextSerializable resultingState = myTask.finalizeIteration(previousState);
		return resultingState;
	}
	@Override
	public void processPreDataBlock(Task configuredTask, TextSerializable previousState) {
		IterativeTask myTask = (IterativeTask)configuredTask;
		myTask.startDataBlock(previousState);
	}

	@Override
	public DataItem processDataItem(Task configuredTask, DataItem dataItem) {
		return null;
	}

	@Override
	public TextSerializable processPostDataBlock(Task configuredTask) {
		IterativeTask myTask = (IterativeTask)configuredTask;
		TextSerializable resultingState = myTask.finalizeDataBlock();
		return resultingState;
		
	}
	@Override
	public String toString() {
		return "IterativeTaskContainer [numberOfIterations=" + numberOfIterations
				+ ", iterationNumber=" + iterationNumber + "]";
	}

	@Override
	public TextSerializable processPreLoop(Task configuredTask, TextSerializable previousState) {
		IterativeTask myTask = (IterativeTask)configuredTask;
		TextSerializable resultingState = myTask.beforeAllIterations(previousState);
		return resultingState;	
	}

	@Override
	public TextSerializable processPostLoop(Task configuredTask, List<TextSerializable> previousState) {
		IterativeTask myTask = (IterativeTask)configuredTask;
		TextSerializable resultingState = myTask.afterAllIterations(previousState);
		return resultingState;	
	}

	@Override
	public List<String> getDataItemTags(DataItem tag) {
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public String toTextRepresentation() {
		JSONObject obj = new JSONObject();
		obj.put("iterations", this.numberOfIterations);
		obj.put("iterationNumber", this.iterationNumber);
		return obj.toString();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void fromTextRepresentation(String textRepresentation) {
		
		JSONParser parser = new JSONParser();
		try {
			Map<String, Long> json = (Map<String, Long>)parser.parse(textRepresentation);
			if (json.get("iterations")!=null) this.numberOfIterations = json.get("iterations").intValue();
			if (json.get("iterationNumber")!=null) this.iterationNumber = json.get("iterationNumber").intValue();
			
		} catch (ParseException e) {
			throw new BIGSException("error parsing JSON representation of "+this.getClass().getName());
		}
	}
	
	
	
}

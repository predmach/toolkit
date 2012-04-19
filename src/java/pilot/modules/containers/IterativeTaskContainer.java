package pilot.modules.containers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import bigs.api.core.BIGSParam;
import bigs.api.exceptions.BIGSException;
import pilot.core.TaskContainer;
import pilot.core.TextSerializable;
import pilot.core.data.LLDDataItem;

public class IterativeTaskContainer extends TaskContainer<IterativeTask> {

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
	public List<TaskContainer<IterativeTask>> generateMyConfiguredTaskContainers() {
		List<TaskContainer<IterativeTask>> r = new ArrayList<TaskContainer<IterativeTask>>();
		for (int i=1; i<= this.numberOfIterations; i++) {
			TaskContainer<IterativeTask> tb = new IterativeTaskContainer(this.numberOfIterations, i);
			r.add(tb);
		}		
		return r;		
	}

	@Override
	public Boolean supportsParallelization() {
		return false;
	}

	@Override
	public TextSerializable processPreSubContainers(IterativeTask configuredTask, TextSerializable previousState) {
		TextSerializable resultingState = configuredTask.startIteration(previousState);
		return resultingState;
	}

	@Override
	public TextSerializable processPostSubContainers(IterativeTask configuredTask, TextSerializable previousState) {
		TextSerializable resultingState = configuredTask.finalizeIteration(previousState);
		return resultingState;
	}
	@Override
	public void processPreDataBlock(IterativeTask configuredTask, TextSerializable previousState) {
		configuredTask.startDataBlock(previousState);
	}

	@Override
	public LLDDataItem processDataItem(IterativeTask configuredTask, LLDDataItem dataItem) {
		return null;
	}

	@Override
	public TextSerializable processPostDataBlock(IterativeTask configuredTask) {
		TextSerializable resultingState = configuredTask.finalizeDataBlock();
		return resultingState;
		
	}
	@Override
	public String toString() {
		return "IterativeTaskContainer [numberOfIterations=" + numberOfIterations
				+ ", iterationNumber=" + iterationNumber + "]";
	}

	@Override
	public TextSerializable processPreLoop(IterativeTask configuredTask, TextSerializable previousState) {
		TextSerializable resultingState = configuredTask.beforeAllIterations(previousState);
		return resultingState;	
	}

	@Override
	public TextSerializable processPostLoop(IterativeTask configuredTask, List<TextSerializable> previousState) {
		TextSerializable resultingState = configuredTask.afterAllIterations(previousState);
		return resultingState;	
	}

	@Override
	public List<String> getDataItemTags(LLDDataItem tag) {
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

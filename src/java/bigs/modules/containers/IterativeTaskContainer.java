package bigs.modules.containers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import bigs.api.core.BIGSParam;
import bigs.api.data.DataItem;
import bigs.api.exceptions.BIGSException;
import bigs.core.pipelines.State;
import bigs.core.pipelines.TaskContainer;

public class IterativeTaskContainer extends TaskContainer<IterativeTask<State,DataItem,DataItem>> {

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
	public List<TaskContainer<IterativeTask<State,DataItem,DataItem>>> generateMyPreparedTaskContainers() {
		List<TaskContainer<IterativeTask<State,DataItem,DataItem>>> r = new ArrayList<TaskContainer<IterativeTask<State,DataItem,DataItem>>>();
		for (int i=1; i<= this.numberOfIterations; i++) {
			TaskContainer<IterativeTask<State,DataItem,DataItem>> tb = new IterativeTaskContainer(this.numberOfIterations, i);
			r.add(tb);
		}		
		return r;		
	}

	@Override
	public Boolean supportsParallelization() {
		return false;
	}

	@Override
	public State processPreSubContainers(IterativeTask<State,DataItem,DataItem> configuredTask, State previousState) {
		State resultingState = configuredTask.startIteration(previousState);
		return resultingState;
	}

	@Override
	public State processPostSubContainers(IterativeTask<State,DataItem,DataItem> configuredTask, State previousState) {
		State resultingState = configuredTask.finalizeIteration(previousState);
		return resultingState;
	}
	@Override
	public void processPreDataBlock(IterativeTask<State,DataItem,DataItem> configuredTask, State previousState) {
		configuredTask.startDataBlock(previousState);
	}

	@Override
	public <I extends DataItem> DataItem processDataItem(IterativeTask<State,DataItem,DataItem> configuredTask, I dataItem) {
		return null;
	}

	@Override
	public State processPostDataBlock(IterativeTask<State,DataItem,DataItem> configuredTask) {
		State resultingState = configuredTask.finalizeDataBlock();
		return resultingState;
		
	}
	@Override
	public String toString() {
		return "IterativeTaskContainer [numberOfIterations=" + numberOfIterations
				+ ", iterationNumber=" + iterationNumber + "]";
	}

	@Override
	public State processPreLoop(IterativeTask<State,DataItem,DataItem> configuredTask, State previousState) {
		State resultingState = configuredTask.beforeAllIterations(previousState);
		return resultingState;	
	}

	@Override
	public State processPostLoop(IterativeTask<State,DataItem,DataItem> configuredTask, List<State> previousState) {
		State resultingState = configuredTask.afterAllIterations(previousState);
		return resultingState;	
	}

	@Override
	public Map<String, String> getDataItemTags(String dataItemRowkey) {
		Double d = Math.random()*this.numberOfIterations+1;
		Map<String, String> r = new HashMap<String, String>();
		
		r.put("iteration", new Integer(d.intValue()).toString());
		return r;
		
	}
	
	@Override
	public Map<String, String> getMyTagsAsPrepared() {
		Map<String, String> r = new HashMap<String, String>();
		r.put("iteration", this.iterationNumber.toString());
		return r;
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
	
	@Override
	public <D extends DataItem> Boolean acceptsEmptyDataItem(IterativeTask<State,DataItem,DataItem> configuredTask, D dataItem) {
		return configuredTask.acceptsEmptyDataItemForIterative(dataItem);
	}	
	
}

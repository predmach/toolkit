package bigs.api.examples.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import bigs.api.core.BIGSException;
import bigs.api.core.BIGSParam;
import bigs.api.core.Task;
import bigs.api.core.TaskContainer;
import bigs.api.data.RawDataItem;
import bigs.api.data.LLDDataItem;
import bigs.api.tasks.DataPartitionTask;
import bigs.core.utils.Log;
import bigs.modules.containers.DataPartitionTaskContainer;

public class DataPartitionExampleTask implements DataPartitionTask<StateWithDoubleValue, RawDataItem, LLDDataItem>{

	@BIGSParam
	public Integer numberOfPartitions;
	
	@Override
	public List<TaskContainer<? extends Task>> getTaskContainerCascade() {
		List<TaskContainer<? extends Task>> r = new ArrayList<TaskContainer<? extends Task>>();
		r.add(new DataPartitionTaskContainer(numberOfPartitions));
		return r;
	}	

	@Override
	public String getDescription() {
		return "This is an example DataPartitionTask";
	}

	@SuppressWarnings("unchecked")
	@Override
	public String toTextRepresentation() {	 
		JSONObject obj = new JSONObject();
		obj.put("partitions", this.numberOfPartitions);
		return obj.toString();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void fromTextRepresentation(String textRepresentation) {
		
		JSONParser parser = new JSONParser();
		try {
			Map<String, Long> json = (Map<String, Long>)parser.parse(textRepresentation);
			if (json.get("partitions")!=null) this.numberOfPartitions = json.get("partitions").intValue();
			
		} catch (ParseException e) {
			throw new BIGSException("error parsing JSON representation of "+this.getClass().getName());
		}
	}

	StateWithDoubleValue instanceState;
	@Override
	public void startPartition(StateWithDoubleValue previousState) {
		instanceState = new StateWithDoubleValue();
		instanceState.value = 0D;
	}

	@Override
	public LLDDataItem processPartitionDataItem(RawDataItem item) {
		Double r = new Double(item.getBytes().length);
		instanceState.value = instanceState.value +r;
		
		List<Double> l = new ArrayList<Double>();
		l.add(r);
		l.add(Math.random()*10);
		l.add(Math.random()*10);

		LLDDataItem rd = new LLDDataItem();
		rd.addLD(l);
		return rd;
	}

	@Override
	public StateWithDoubleValue finalizePartition() {
		Log.info("returning "+instanceState.toTextRepresentation());
		return instanceState;
	}

	@Override
	public StateWithDoubleValue beforeProcessingAllPartitions(StateWithDoubleValue previousState) {
		return previousState;
	}

	@Override
	public StateWithDoubleValue afterProcessingAllPartitions(List<StateWithDoubleValue> previousStates) {
		StateWithDoubleValue s = new StateWithDoubleValue();
		s.value = 0D;
		for (StateWithDoubleValue p: previousStates) {
			s.value = s.value + p.value;
		}
		
		Log.info("accumulated "+s.value);
		return s;
	}

	@Override
	public Boolean acceptsEmptyDataItemForPartition(RawDataItem dataItem) {
		return true;
	}

	@Override
	public StateWithDoubleValue beforeProcessingPartitionSubContainers(StateWithDoubleValue previousState) {
		return previousState;
	}

	@Override
	public StateWithDoubleValue afterProcessingPartitionSubContainers(StateWithDoubleValue previousState) {
		return previousState;
	}

}

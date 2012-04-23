package bigs.core.pipelines;

import java.util.List;
import java.util.Map;

import bigs.core.data.DataItem;


public class TopLevelTaskContainer extends TaskContainer<Task> {

	public TopLevelTaskContainer(){}
	
	public TopLevelTaskContainer(PipelineStage pipelineStage) {
		this.setPipelineStage(pipelineStage);
	}
	
	@Override
	public List<TaskContainer<Task>> generateMyPreparedTaskContainers() {
		return null;
	}

	@Override
	public Boolean supportsParallelization() {
		return null;
	}

	@Override
	public State processPreSubContainers(Task configuredTask, State previousState) {
		return null;
	}

	@Override
	public State processPostSubContainers(Task configuredTask, State previousState) {
		return null;
	}

	@Override
	public State processPreLoop(Task configuredTask, State previousState) {
		return null;
	}

	@Override
	public State processPostLoop(Task configuredTask, List<State> previousState) {
		return null;
	}

	@Override
	public void processPreDataBlock(Task configuredTask, State previousState) {
		
	}

	@Override
	public DataItem processDataItem(Task configuredTask, DataItem dataItem) {
		return null;
	}

	@Override
	public State processPostDataBlock(Task configuredTask) {
		return null;
		
	}

	@Override
	public String toString() {
		return "TopLevelTaskContainer []";
	}
	
	@Override
	public Map<String, String> getMyTagsAsPrepared() {
		return null;
	}

	@Override
	public Map<String, String> getDataItemTags(String dataItemRowkey) {
		return null;
	}

	@Override
	public String toTextRepresentation() {
		return "";
	}

	@Override
	public void fromTextRepresentation(String textRepresentation) {
	}

}

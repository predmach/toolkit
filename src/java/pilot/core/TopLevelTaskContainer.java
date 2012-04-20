package pilot.core;

import java.util.List;

import pilot.core.data.DataItem;
import pilot.core.data.LLDDataItem;

public class TopLevelTaskContainer extends TaskContainer<Task> {

	public TopLevelTaskContainer(){}
	
	public TopLevelTaskContainer(PipelineStage pipelineStage) {
		this.setPipelineStage(pipelineStage);
	}
	
	@Override
	public List<TaskContainer<Task>> generateMyConfiguredTaskContainers() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Boolean supportsParallelization() {
		// TODO Auto-generated method stub
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
	public List<String> getDataItemTags(LLDDataItem tag) {
		// TODO Auto-generated method stub
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

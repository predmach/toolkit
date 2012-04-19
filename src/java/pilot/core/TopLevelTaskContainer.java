package pilot.core;

import java.util.List;

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
	public TextSerializable processPreSubContainers(Task configuredTask, TextSerializable previousState) {
		return null;
	}

	@Override
	public TextSerializable processPostSubContainers(Task configuredTask, TextSerializable previousState) {
		return null;
	}

	@Override
	public TextSerializable processPreLoop(Task configuredTask, TextSerializable previousState) {
		return null;
	}

	@Override
	public TextSerializable processPostLoop(Task configuredTask, List<TextSerializable> previousState) {
		return null;
	}

	@Override
	public void processPreDataBlock(Task configuredTask, TextSerializable previousState) {
		
	}

	@Override
	public LLDDataItem processDataItem(Task configuredTask, LLDDataItem dataItem) {
		return null;
	}

	@Override
	public TextSerializable processPostDataBlock(Task configuredTask) {
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

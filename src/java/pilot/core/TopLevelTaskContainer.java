package pilot.core;

import java.util.List;

public class TopLevelTaskContainer extends TaskContainer {

	public TopLevelTaskContainer(){}
	
	public TopLevelTaskContainer(PipelineStage pipelineStage) {
		this.setPipelineStage(pipelineStage);
	}
	
	@Override
	public List<Class<? extends TaskContainer>> allowedTaskContainers() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Class<? extends Task>> allowedTasks() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<TaskContainer> generateMyTaskContainers() {
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
	public DataItem processDataItem(Task configuredTask, DataItem dataItem) {
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
	public List<String> getDataItemTags(DataItem tag) {
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

package pilot.core;

import java.util.List;

public class TopLevelTaskContainer extends TaskContainer {

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
	public TextSerializable processPreSubContainers(TextSerializable previousState) {
		return null;
	}

	@Override
	public TextSerializable processPostSubContainers(TextSerializable previousState) {
		return null;
	}

	@Override
	public TextSerializable processPreLoop(TextSerializable previousState) {
		return null;
	}

	@Override
	public TextSerializable processPostLoop(List<TextSerializable> previousState) {
		return null;
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
		return "TopLevelTaskContainer []";
	}

	@Override
	public List<String> getDataItemTags(DataItem tag) {
		// TODO Auto-generated method stub
		return null;
	}

}

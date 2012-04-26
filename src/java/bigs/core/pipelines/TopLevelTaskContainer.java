package bigs.core.pipelines;

import java.util.List;
import java.util.Map;

import bigs.api.core.BIGSException;
import bigs.api.core.State;
import bigs.api.core.Task;
import bigs.api.core.TaskContainer;
import bigs.api.data.DataItem;
import bigs.api.storage.DataSource;
import bigs.api.storage.Result;
import bigs.api.storage.ResultScanner;
import bigs.api.storage.Scan;
import bigs.api.storage.Table;
import bigs.api.storage.Update;
import bigs.core.utils.Log;


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
		PipelineStage stage = this.getPipelineStage();
		Log.info("tagging data items for stage "+stage.getStageNumber()+" ... ");
		DataSource dataSource = stage.getPreparedInputDataSource();
		Table table = dataSource.getTable(stage.getInputTableName());
		Scan scan = table.createScanObject();
		scan.addFamily("tags");
		scan.addFamily("bigs");
		scan.addColumn("content", "class");
		ResultScanner rs = table.getScan(scan);
		try {
			for (Result rr = rs.next(); rr!=null; rr = rs.next()) {					
				
				DataItem dataItem = DataItem.fromResult(rr, DataItem.EMPTY_OBJECT);
				
				String key = rr.getRowKey();
		    	Update update = table.createUpdateObject(key);

		    	List<TaskContainer<? extends Task>> containers = stage.getPreparedTask().getTaskContainerCascade();

		    	// last task container must accept the data item
		    	TaskContainer lastContainer = containers.get(containers.size()-1);
		    	
	    		Task preparedTask = stage.getPreparedTask();
	    		try {
	    			Boolean accepts = lastContainer.acceptsEmptyDataItem(preparedTask, dataItem);	    			
	    			if (!accepts) throw new BIGSException("refused by Task definition");
	    		} catch (Exception e) {
	    			throw new BIGSException("data item "+key+", not accepted by "+preparedTask.getClass().getName()+", "+e.getMessage());
	    		}

	    		for (TaskContainer<? extends Task> container: containers) {
		    		
		    		
		    		container.setPipelineStage(stage);
		    		Map<String, String> tags = container.getFQNDataItemTags(key);
		    		if (tags!=null) {
			    		for (String tagName: tags.keySet()) {
			    			String tagValue = tags.get(tagName);
			    			update.add("tags", tagName, tagValue.getBytes());
			    		}
		    		}
		    	}
		    	
		    	table.update(update);
			}
		} finally {
			rs.close();
		}		
		Log.info("data items tagged for stage "+stage.getStageNumber());
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

	@Override
	public <D extends DataItem> Boolean acceptsEmptyDataItem(Task configuredTask, D dataItem) {
		return true;
	}
}
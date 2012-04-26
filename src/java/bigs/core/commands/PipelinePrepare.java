package bigs.core.commands;

import java.util.List;
import java.util.Map;


import bigs.api.core.BIGSException;
import bigs.api.core.Task;
import bigs.api.core.TaskContainer;
import bigs.api.data.DataItem;
import bigs.api.storage.DataSource;
import bigs.api.storage.Result;
import bigs.api.storage.ResultScanner;
import bigs.api.storage.Scan;
import bigs.api.storage.Table;
import bigs.api.storage.Update;
import bigs.core.pipelines.Pipeline;
import bigs.core.pipelines.PipelineStage;
import bigs.core.pipelines.Schedule;
import bigs.core.pipelines.ScheduleItem;
import bigs.core.utils.Log;
import bigs.core.utils.Text;


public class PipelinePrepare extends Command {

	@Override
	public String getCommandName() {		
		return "pipeline.prepare";
	}

	@Override
	public String[] getHelpString() {
		return new String[]{"'bigs "+this.getCommandName()+" pipelineNumber'"};
	}

    @Override
    public Boolean checkCallingSyntax(String[] args) {
    	if (args.length!=1) return false;
		try {
			new Long(args[0]);
		} catch (NumberFormatException e) {
			return false;
		}
    	return true;
    }

    @Override
	public void run(String[] args) {
		
		Integer pipelineNumber = new Integer(args[0]);
		Pipeline pipeline = Pipeline.fromPipelineNumber(pipelineNumber);

		List<PipelineStage> stages = pipeline.getStages();
		if (stages.size()==0) {
			throw new BIGSException("no stages defined in pipeline file");
		}
		ScheduleItem lastScheduleItem = null;
		
		for (PipelineStage stage: stages) {
			Log.info("Processing stage number "+stages.indexOf(stage));
			
			// first generate the schedule
			Schedule schedule = stage.generateSchedule();
			if (lastScheduleItem!=null) {
				schedule.getFirst().addParentRowkey(lastScheduleItem.getRowkey());
			}			
			schedule.save();    
			lastScheduleItem = schedule.getLast();
			Log.info("Pipeline "+pipelineNumber+" generated and saved "+schedule.getItems().size()+" schedule item ");
			
			// then tag data items
			Log.info("tagging data items ... ");
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
			Log.info("data items tagged");
		}
		
    	// ----------------------------------
    	pipeline.setStatus(Pipeline.STATUS_ACTIVE);
    	pipeline.setTimeDone(null);
    	pipeline.setTimeStart(null);
    	pipeline.save();
    	Log.info("pipeline marked as active");
	}

	@Override
	public String getDescription() {
		return "creates the schedule for a pipeline, leaving it ready for workers";
	}

}

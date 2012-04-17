package bigs.core.commands;

import java.util.List;

import pilot.core.PipelineStage;
import pilot.core.Schedule;

import bigs.api.exceptions.BIGSException;
import bigs.core.explorations.Pipeline;
import bigs.core.utils.Log;


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
		Log.info("Processing only stage one");
		PipelineStage stage = stages.get(0);
		Schedule schedule = stage.generateSchedule();
		schedule.save();
		
    	
		Log.info("Pipeline "+pipelineNumber+" generated and saved "+schedule.getItems().size()+" schedule item ");
		
    	// marks the source dataset for splitting
		Log.info("****** MUST IMPLEMENT TAG PHASE *****");
    	Log.info("marked data splits in source dataset");
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

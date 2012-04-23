package bigs.core.commands;

import java.util.Arrays;

import bigs.core.pipelines.Pipeline;
import bigs.core.utils.Log;


public class ExplorationStatus  extends Command {

	@Override
	public String getCommandName() {		
		return "exploration.status";
	}

	@Override
	public String[] getHelpString() {
		String[] allowed = new String[Pipeline.STATUS_STRINGS.length-1];
		for (int i=0; i<Pipeline.STATUS_STRINGS.length-1; i++) {
			allowed[i] = Pipeline.STATUS_STRINGS[i+1];
		}
		return new String[]{"'bigs "+this.getCommandName()+" status'","",
				            " where status is one of "+Arrays.toString(allowed)};
	}

    @Override
    public Boolean checkCallingSyntax(String[] args) {
    	return (args.length==2);
    }

    @Override
	public void run(String[] args) {
    	Integer explorationNumber = new Integer(args[0]);
    	Pipeline expl = Pipeline.fromPipelineNumber(explorationNumber);
    	expl.setStatusFromString(args[1]);
    	if (expl.isStatusNone()) {
    		Log.error("status "+args[1]+" not valid");
    	}
    	expl.save();
    	Log.info("exploration is now "+expl.toString());
    	
	}
    
	@Override
	public String getDescription() {
		return "sets the status of an exploration";
	}

}
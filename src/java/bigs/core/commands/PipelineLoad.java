package bigs.core.commands;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import bigs.api.core.BIGSException;
import bigs.core.pipelines.Pipeline;
import bigs.core.utils.Log;


public class PipelineLoad extends Command {

	@Override
	public String getCommandName() {		
		return "pipeline.load";
	}

	@Override
	public String[] getHelpString() {
		return new String[]{"'bigs "+this.getCommandName()+" pipelineFile'"};
	}

    @Override
    public Boolean checkCallingSyntax(String[] args) {
    	return (args.length==1);
    }

    @Override
	public void run(String[] args) {
		File props = new File(args[0]);
		PipelineLoad.loadPipeline(props);
	}
    
    public static void loadPipeline(File pipelineFile) {
		try {
			Log.info("using pipeline properties file at "+pipelineFile.toString());

			Pipeline expl = new Pipeline();
			expl.load(new FileReader(pipelineFile));
			expl.save();
			Log.info("saved pipeline "+expl.toString());
			
		} catch (FileNotFoundException e) {
			throw new BIGSException("file "+pipelineFile.toString()+" not found");
		} 
    	
    }

	@Override
	public String getDescription() {
		return "processes and loads a pipeline into BIGS";
	}

}

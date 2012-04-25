package bigs.core.commands;

import java.lang.reflect.Modifier;
import java.util.List;

import bigs.api.fe.FeatureExtractionTask;
import bigs.core.pipelines.TaskHelper;
import bigs.core.utils.Core;
import bigs.core.utils.Log;


public class ShowFeatureExtractors  extends Command {

	@Override
	public String getCommandName() {
		return "show.fe";
	}

	@Override
	public String[] getHelpString() {
		return new String[]{ "'bigs "+getCommandName() };
	}

	@Override
	public void run(String[] args) throws Exception {		
		
		Log.info("scanning libraries ...");
		
		List<Class<? extends FeatureExtractionTask>> l = Core.getAllSubclasses(FeatureExtractionTask.class);

		if (l.size()==0) {
			Log.info("no classes found");
		}
		for (Class<? extends FeatureExtractionTask> c: l) {
    		if (!Modifier.isAbstract(c.getModifiers())) {
    			FeatureExtractionTask alg = c.newInstance();
    			for (String ss: TaskHelper.getHelp(alg)) {
    				System.out.println("     "+ ss);
    			}
    			System.out.println();			
    		}
		}
	}

	@Override
	public String getDescription() {
		return "shows available features extractors";
	}

	@Override
	public Boolean checkCallingSyntax(String[] args) {
		if (args.length!=0) return false;
		return true;
	}

}

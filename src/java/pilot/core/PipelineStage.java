package pilot.core;

import java.util.ArrayList;
import java.util.List;

import bigs.api.exceptions.BIGSException;
import bigs.core.exceptions.BIGSPropertyNotFoundException;
import bigs.core.explorations.Exploration;
import bigs.core.utils.Core;
import bigs.core.utils.Log;
import bigs.core.utils.Text;


public class PipelineStage {

	public static String lprefix = "stage";
	
	Integer stageNumber = 1;
	TaskContainer topLevelContainer = new TopLevelTaskContainer(this);
	Task configuredTask;
	Exploration exploration;

	public PipelineStage (Exploration exploration, Integer stageNumber) {
		this.stageNumber = stageNumber;
		this.exploration = exploration;
		
		this.generateTaskContainerCascade();
	}
	

	public Task getConfiguredTask() {
		return configuredTask;
	}
	
	public Integer getStageNumber() {
		return this.stageNumber;
	}

	List<TaskContainer> generateTaskContainers (List<TaskContainer> containerCascade, Integer level) {
		List<TaskContainer> r = new ArrayList<TaskContainer>();

		if (level>=containerCascade.size()) {
			return r;
		}
		
		TaskContainer container = containerCascade.get(level);

		for (TaskContainer tc: container.generateMyTaskContainers()) {
			r.add(tc);
			tc.setPipelineStage(this);
			List<TaskContainer> subContainers = this.generateTaskContainers(containerCascade, level+1);				
			if (subContainers!=null) {
				for (TaskContainer stc: subContainers) {
					if (tc.allowsTaskContainer(stc.getClass())) {
						tc.addTaskContainer(stc);
					} else {
						throw new BIGSException(tc.getClass().getSimpleName()+" does not allow task containers of type "+stc.getClass().getSimpleName());
					}
				}
			}
		}
	
		return r;
	}
	
	void generateTaskContainerCascade() {
		String stagePrefix = lprefix+"."+Text.zeroPad(new Long(stageNumber), 2);

		try {
			this.configuredTask = Core.getConfiguredObject("task", Task.class, exploration, stagePrefix);
			this.topLevelContainer = new TopLevelTaskContainer(this);
			List<TaskContainer> containers = this.generateTaskContainers(this.configuredTask.getTaskContainerCascade(),0);
			for (TaskContainer c: containers) {
				this.topLevelContainer.addTaskContainer(c);
			}
			
			return;
		} catch (BIGSPropertyNotFoundException e) {
			throw new BIGSException(e.getMessage());
		}
		
	}
	


	
	@Override
	public String toString() {
		return "PipelineStage [stageNumber=" + stageNumber
				+ ", topLevelContainer=" + topLevelContainer
				+ ", configuredTask=" + configuredTask + ", sourceProperties="
				+ "]";
	}

	public Schedule generateSchedule() {
		Log.info("Stage "+this.stageNumber);
		Log.info("configured task: "+this.configuredTask.toString());
		Schedule schedule = new Schedule(this);
		this.topLevelContainer.fillSchedule(schedule, null);
		return schedule;
	}
	
	public void printOut() {
		Log.info("Stage "+this.stageNumber);
		Log.info("configured task: "+this.configuredTask.toString());
		this.topLevelContainer.printOut("   ");
	}
	
}

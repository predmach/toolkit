package pilot.core;

import java.util.ArrayList;
import java.util.List;

import bigs.api.exceptions.BIGSException;
import bigs.core.exceptions.BIGSPropertyNotFoundException;
import bigs.core.explorations.Pipeline;
import bigs.core.utils.Core;
import bigs.core.utils.Log;
import bigs.core.utils.Text;


public class PipelineStage {

	public static String lprefix = "stage";
	
	Integer stageNumber = 1;
	TaskContainer topLevelContainer = new TopLevelTaskContainer(this);
	Task configuredTask;
	Pipeline pipeline;

	public PipelineStage (Pipeline exploration, Integer stageNumber) {
		this.stageNumber = stageNumber;
		this.pipeline = exploration;
		String stagePrefix = lprefix+"."+Text.zeroPad(new Long(stageNumber), 2);
		try {
			this.configuredTask = Core.getConfiguredObject("task", Task.class, exploration, stagePrefix);
		} catch (BIGSPropertyNotFoundException e) {
			throw new BIGSException(e.getMessage());
		}
		
	}
	
	public Pipeline getPipeline() {
		return this.pipeline;
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

			this.topLevelContainer = new TopLevelTaskContainer(this);
			List<TaskContainer> containerCascade = this.configuredTask.getTaskContainerCascade();

			// checks if the declared containers accept the task that wants to use them
			for (TaskContainer container: containerCascade) {
				if (!container.allowsTask(configuredTask)) {
					throw new BIGSException("container "+container.getClass().getName()+" does not allow tasks of type "+configuredTask.getClass().getName());
				}
			}

			List<TaskContainer> configuredContainers = this.generateTaskContainers(containerCascade,0);
			
			
			for (TaskContainer c: configuredContainers) {
				this.topLevelContainer.addTaskContainer(c);
			}
			
			return;
	}
	


	
	@Override
	public String toString() {
		return "PipelineStage [stageNumber=" + stageNumber
				+ ", topLevelContainer=" + topLevelContainer
				+ ", configuredTask=" + configuredTask + ", sourceProperties="
				+ "]";
	}

	/**
	 * generates the schedule corresponding to this stage from the task
	 * defined in the pipeline properties file
	 * 
	 * @return the schedule
	 */
	public Schedule generateSchedule() {
		this.generateTaskContainerCascade();
		Log.info("Stage "+this.stageNumber);
		Log.info("configured task: "+this.configuredTask.toString());
		Schedule schedule = new Schedule(this);
		this.topLevelContainer.fillSchedule(schedule, null);
		return schedule;
	}
	
	/**
	 * loads the schedule of this stage form the underlying storage
	 * @return
	 */
	public Schedule loadSchedule() {
		return Schedule.load(this);
		
	}
	
	
	
	public void printOut() {
		Log.info("Stage "+this.stageNumber);
		if (this.configuredTask!=null) Log.info("configured task: "+this.configuredTask.toString());
		this.topLevelContainer.printOut("   ");
	}
	
}

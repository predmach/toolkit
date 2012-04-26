package bigs.core.pipelines;

import java.util.ArrayList;
import java.util.List;

import bigs.api.core.BIGSException;
import bigs.api.core.Task;
import bigs.api.core.TaskContainer;
import bigs.api.storage.DataSource;
import bigs.core.exceptions.BIGSPropertyNotFoundException;
import bigs.core.utils.Core;
import bigs.core.utils.Log;
import bigs.core.utils.Text;


public class PipelineStage {

	public static String lprefix = "stage";
	
	Integer stageNumber = 1;
	String stagePrefix = "";
	TaskContainer<Task> topLevelContainer = new TopLevelTaskContainer(this);
	Task preparedTask;
	Pipeline pipeline;

	public PipelineStage (Pipeline exploration, Integer stageNumber) {
		this.stageNumber = stageNumber;
		this.pipeline = exploration;
		stagePrefix = lprefix+"."+Text.zeroPad(new Long(stageNumber), 2);
		try {
			Object obj = Core.getPreparedObject("task", Task.class, exploration, stagePrefix);
			this.preparedTask = Core.getPreparedObject("task", Task.class, exploration, stagePrefix);
		} catch (BIGSPropertyNotFoundException e) {
			throw new BIGSException(e.getMessage());
		}
		
	}
	
	public Pipeline getPipeline() {
		return this.pipeline;
	}
	
	public Task getPreparedTask() {
		return preparedTask;
	}
	
	public Integer getStageNumber() {
		return this.stageNumber;
	}

	List<TaskContainer<? extends Task>> generateTaskContainers (List<TaskContainer<? extends Task>> containerCascade, Integer level) {
		List<TaskContainer<? extends Task>> r = new ArrayList<TaskContainer<? extends Task>>();

		if (level>=containerCascade.size()) {
			return r;
		}
		
		TaskContainer<? extends Task> container = containerCascade.get(level);

		for (TaskContainer<? extends Task> tc: container.generateMyPreparedTaskContainers()) {
			r.add(tc);
			tc.setPipelineStage(this);
			List<TaskContainer<? extends Task>> subContainers = this.generateTaskContainers(containerCascade, level+1);				
			if (subContainers!=null) {
				for (TaskContainer<? extends Task> stc: subContainers) {
					tc.addTaskContainer(stc);
				}
			}
		}
	
		return r;
	}
	
	void generateConfiguredTaskContainerCascade() {

			this.topLevelContainer = new TopLevelTaskContainer(this);
			List<TaskContainer<? extends Task>> containerCascade = this.preparedTask.getTaskContainerCascade();

			List<TaskContainer<? extends Task>> configuredContainers = this.generateTaskContainers(containerCascade,0);
			
			
			for (TaskContainer<? extends Task> c: configuredContainers) {
				this.topLevelContainer.addTaskContainer(c);
			}
			
			return;
	}
	
	public String getInputTableName() {
		return this.pipeline.getProperty(this.stagePrefix+".input.table");
	}
	
	public String getOutputTableName() {
		return this.pipeline.getProperty(this.stagePrefix+".output.table");
	}

	public DataSource getPreparedInputDataSource() {
		DataSource r;
		try {
			r = Core.getPreparedObject(
					"source", DataSource.class, this.pipeline, this.stagePrefix+".input");
		} catch (BIGSPropertyNotFoundException e) {
			throw new BIGSException("error in properties: "+e.getMessage());
		}
		
		r.initialize();
		return r;
	}
	
	public DataSource getPreparedOutputDataSource() {
		DataSource r;
		try {
			r = Core.getPreparedObject(
					"source", DataSource.class, this.pipeline, this.stagePrefix+".output");
		} catch (BIGSPropertyNotFoundException e) {
			throw new BIGSException("error in properties: "+e.getMessage());
		}		
		r.initialize();
		return r;
	}

	
	@Override
	public String toString() {
		return "PipelineStage [stageNumber=" + stageNumber
				+ ", topLevelContainer=" + topLevelContainer
				+ ", configuredTask=" + preparedTask + ", sourceProperties="
				+ "]";
	}

	/**
	 * generates the schedule corresponding to this stage from the task
	 * defined in the pipeline properties file
	 * 
	 * @return the schedule
	 */
	public Schedule generateSchedule() {
		this.generateConfiguredTaskContainerCascade();
		Log.info("Stage "+this.stageNumber);
		Log.info("configured task: "+this.preparedTask.toString());
		Schedule schedule = new Schedule(this);
		this.topLevelContainer.fillSchedule(schedule, null, null);
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
		if (this.preparedTask!=null) Log.info("configured task: "+this.preparedTask.toString());
		this.topLevelContainer.printOut("   ");
	}
	
}

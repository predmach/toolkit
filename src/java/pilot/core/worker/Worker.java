package pilot.core.worker;
import java.util.ArrayList;
import java.util.List;

import pilot.core.Schedule;
import pilot.core.ScheduleItem;
import pilot.core.Task;
import pilot.core.TaskContainer;
import pilot.core.TextSerializable;

import bigs.api.exceptions.BIGSException;
import bigs.api.storage.DataSource;
import bigs.api.storage.Put;
import bigs.api.storage.Table;
import bigs.core.BIGS;
import bigs.core.BIGSProperties;
import bigs.core.explorations.Evaluation;
import bigs.core.explorations.Pipeline;
import bigs.core.utils.Core;
import bigs.core.utils.Data;
import bigs.core.utils.Log;
import bigs.core.utils.Text;


public class Worker {
	
	public ScheduleItem currentScheduleItem = null;
	
	Boolean abort = false;
	


	
	ScheduleItem selectNextScheduleItem() {
		
		// retrieve the pipelines which are active in the DB
    	List<Pipeline> activePipelines = Pipeline.getPipelinesForStatus(Pipeline.STATUS_ACTIVE);
    	if (activePipelines == null || activePipelines.size()==0) {
    		return null;
    	}
    	
    	// Look what pipeline still has schedule items pending
    	for (Pipeline pipeline: activePipelines) {
    		Schedule schedule = pipeline.getStages().get(0).loadSchedule();
    		
    		// check if all schedule items are done and mark the pipeline done if so
    		Integer unfinishedScheduleItems=0;
    		for (ScheduleItem si: schedule.getItems()) {
    			if (!si.isStatusDone()) unfinishedScheduleItems ++;
    		}
    		// if the pipeline is active and all schedule items are done, mark the pipeline as done
    		if (unfinishedScheduleItems.equals(0)) {
    			pipeline.setStatus(Pipeline.STATUS_DONE);
    			pipeline.setTimeDoneFromTimeReference();
    			pipeline.save();
    			continue;
    		}

    		// Now select which schedule item within the pipeline
    		for (ScheduleItem scheduleItem: schedule.getItems()) {
    			if (scheduleItem.canProcess()) return scheduleItem;												
    		}
    		
    		// if we got here no schedule item was selected and we move over next pipeline
    	}
    	return null;
		
	}
	
	/**
	 * starts this worker in a continuous loop checking regularly for evaluations to do
	 */
	public void start() {
		Log.info("starting worker "+this.toString());
        AliveThread athread = new AliveThread(this);
        athread.start();

        Log.info("[time alive "+Text.timeToString(BIGSProperties.WORKER_ALIVE_INTERVAL)+"] ");
        Log.info("[time sleep "+Text.timeToString(BIGSProperties.WORKER_SLEEP_INTERVAL)+"] ");
        Log.info("[time clean "+Text.timeToString(BIGSProperties.WORKER_CLEAN_INTERVAL)+"] ");
        
        while (true) {

        	try {
	        	ScheduleItem scheduleItem = this.selectNextScheduleItem();
	    		if (scheduleItem!=null) {
					// set status to INPROGRESS and take over the split
					Table evalTable = BIGS.globalProperties.getConfiguredDataSource().getTable(ScheduleItem.tableName);
					Put put = evalTable.createPutObject(scheduleItem.getRowKey());
					scheduleItem.setStatusInProgress();
					put = scheduleItem.fillPutObject(put);
					put = Data.fillInHostMetadata(put);
					Boolean success = evalTable.checkAndPut(scheduleItem.getRowKey(), "bigs", "status", Evaluation.getStatusString(Evaluation.STATUS_PENDING).getBytes(), put);
					// if we did not succeed in setting the status to pending it is because somebody else did and it is working on it
					if (success) {
						Pipeline pipeline = scheduleItem.getSchedule().getPipelineStage().getPipeline();
						
						if (scheduleItem.getId().equals(0)) {
							pipeline.setTimeStartFromTimeReference();
							pipeline.setTimeDone(null);
							pipeline.save();
						}
						this.doScheduleItem(scheduleItem);
						if (!abort) {
							scheduleItem.setStatusDone();
							scheduleItem.save();
						} else {
							Log.info("schedule item aborted");
							abort = false;
						}
	        		}        			
	        	} else {
	            	Log.info("no available schedule items found. will look again in a while");
	            	Core.sleep(BIGSProperties.WORKER_SLEEP_INTERVAL);
	        	}        	
        	} catch (Exception e) {
        		Log.error("worker catched exception "+e.getClass().getName()+", "+e.getMessage());
        		e.printStackTrace();
        		Log.error("will wait a while and start looking to something to do again");
        		Core.sleep(BIGSProperties.WORKER_ALIVE_INTERVAL);
        	}
        }
	}


	
	public void doScheduleItem(ScheduleItem scheduleItem) {
		
		currentScheduleItem = scheduleItem;

		String methodName = scheduleItem.getMethodName();
		TaskContainer configuredContainer = scheduleItem.getConfiguredTaskContainer();
		Task configuredTask = scheduleItem.getConfiguredTask();
		Schedule schedule = scheduleItem.getSchedule();
		
		Log.info("doing "+scheduleItem.toString());
		
		if (!scheduleItem.getMethodName().equals("postLoop")) {
			List<Integer> parentsIds = scheduleItem.getParentsIds();
			if (parentsIds.size()>1) {
				throw new BIGSException("schedule item "+scheduleItem.toString()+" can only have one parent and it has "+parentsIds.size());
			}
	
			TextSerializable previousState = null;
			
			Integer parentId = null;
			if (parentsIds.size()>0) parentId = parentsIds.get(0);
			if (parentId!=null) previousState = schedule.get(parentId).getProcessState();
			
			TextSerializable resultState = null;
			
			
			if (methodName.equals("preSubContainers")) {				
				resultState = configuredContainer.processPreSubContainers(configuredTask, previousState);
			} else if (methodName.equals("postSubContainers")) {
				resultState = configuredContainer.processPostSubContainers(configuredTask, previousState);				
			} else if (methodName.equals("LOOP processDataItem")) {
				configuredContainer.processPreDataBlock(configuredTask, previousState);
				System.out.println("**** INSERT PROCESSING DATA ITEMS ***");
				resultState = configuredContainer.processPostDataBlock(configuredTask);
			} else if (methodName.equals("preLoop")) {
				resultState = configuredContainer.processPreLoop(configuredTask, previousState);
			}
			
			scheduleItem.setProcessState(resultState);
		} else if (methodName.equals("postLoop")){
			List<Integer> parentsIds = scheduleItem.getParentsIds();
			List<TextSerializable> parentsStates = new ArrayList<TextSerializable>();
			for (Integer i: parentsIds) {
				ScheduleItem parent = schedule.get(i);
				if (parent==null) {
					parentsStates.add(null);
				} else {
					parentsStates.add(parent.getProcessState());
				}
			}
			
			TextSerializable resultState = configuredContainer.processPostLoop(configuredTask, parentsStates);
			scheduleItem.setProcessState(resultState);
		}
		
		Long minTime = 2L;
		Long maxTime = 5L;
		Long elapsedTime = minTime + new Double(Math.random()*( maxTime.doubleValue()-minTime.doubleValue())).longValue();
		Core.sleep(elapsedTime * 1000L);
		
		
	}
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("worker, uuid=").append(Core.myUUID);
		return sb.toString();
	}
	
	
	class AliveThread extends Thread {
        Worker worker;  
        Boolean stop = false;
        
        AliveThread (Worker w) {
                worker = w;
        }
        
        @Override
        public void run() {        	
                while (!stop) {
                        Core.sleep(BIGSProperties.WORKER_ALIVE_INTERVAL);
                        DataSource bigsDataSource = BIGS.globalProperties.getConfiguredDataSource();
                        if (abort) {
                        	Log.debug("abort programeed. skipping worker alive update");
                        	continue;
                        }
                        if (bigsDataSource!=null && worker.currentScheduleItem!=null) {                        	
                                synchronized(worker.currentScheduleItem) {
                                	ScheduleItem storedScheduleItem = ScheduleItem.load(BIGS.globalProperties.getConfiguredDataSource(), currentScheduleItem.getSchedule(), currentScheduleItem.getRowKey());
                                	if (!storedScheduleItem.getUuidStored().equals(Core.myUUID)) {
                                		Log.error("current schedule item has been updated by another worker. stopping whenever possible. ");
                                	} else if (currentScheduleItem!=null) {
                                		worker.currentScheduleItem.markAlive(bigsDataSource);                           
                                	}
                                }
                                Log.debug("worker alive updated in schedule item "+worker.currentScheduleItem.getRowKey());
                        }
                }               
        }
        
        public void stopRunning() {
                stop = true;
        }
	}	
	
}
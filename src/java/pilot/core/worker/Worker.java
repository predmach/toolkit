package pilot.core.worker;
import java.util.List;

import pilot.core.Schedule;
import pilot.core.ScheduleItem;

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
    	
    	// Look what exploration still has schedule item pending
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
        		Log.error("will wait a while and start looking to something to do again");
        		Core.sleep(BIGSProperties.WORKER_ALIVE_INTERVAL);
        	}
        }
	}


	
	public void doScheduleItem(ScheduleItem scheduleItem) {
		
		currentScheduleItem = scheduleItem;
		Log.info("DUMMY EXECUTION OF "+scheduleItem.toString());
		Long minTime = 5L;
		Long maxTime = 20L;
		Long elapsedTime = minTime + new Double(Math.random()*( maxTime.doubleValue()-minTime.doubleValue())).longValue();
		Core.sleep(elapsedTime * 1000L);
		
/*		
		Log.info("worker on eval "+eval.toString());
		currentEvaluation = eval;

		Integer stageNumber = eval.getStageNumber().intValue()-1;
		ExplorationStage stage = eval.getParentExploration().getStages().get(stageNumber);
		DataSource originDataSource    = stage.getConfiguredOriginDataSource();
		String     originDataTableName = stage.getOriginContainerName();
		DataSource destinationDataSource    = stage.getConfiguredDestinationDataSource();
		String     destinationDataTableName = stage.getDestinationContainerName();
				
		Algorithm algorithm = eval.getConfiguredAlgorithm();
		
		Data.createDataTableIfDoesNotExist(destinationDataSource, destinationDataTableName);

		Table originTable = originDataSource.getTable(originDataTableName);
		Table destinationTable = destinationDataSource.getTable(destinationDataTableName);
				
		Scan scan = originTable.createScanObject();
		for (String family: Data.dataTableColumnFamilies) {
			scan.addFamily(family);
		}
		scan.setFilterByColumnValue("splits", Text.zeroPad(eval.getExplorationNumber()), eval.getSplitNumber().toString().getBytes());
		ResultScanner rs = originTable.getScan(scan);
		try {
			for (Result rr = rs.next(); rr!=null; rr = rs.next()) {						
				Log.info("      input  rowkey "+rr.getRowKey());

				byte[] bytes = rr.getValue("content", "data");

				Date startTime = new Date();
				//---------------------------------------
				// This is the actual algorithm running
				byte[] result = algorithm.run(bytes);
				//---------------------------------------
				if (abort) {
					Log.error("aborting this evaluation: "+eval.getRowKey());
					return;
				}
				Date endTime = new Date();
				
				eval.addToElapsedTime(endTime.getTime() - startTime.getTime());

				String destinationRowKey = eval.getRowKey()+":"+rr.getRowKey();
				if (algorithm.outputDataRowkeyPrefix()==Algorithm.ROWKEYPREFIX_EXPLORATION_CONFIG_STAGE) {
					destinationRowKey = Text.zeroPad(eval.getExplorationNumber())+"."+
				                        Text.zeroPad(eval.getConfigNumber())+"."+
				                        Text.zeroPad(eval.getStageNumber())+":"+
				                        rr.getRowKey();					
				}				
				Log.info("      output rowkey "+destinationRowKey);
				
				Put put = destinationTable.createPutObject(destinationRowKey);
				put.add("content", "data", result);
				put = Data.fillInHostMetadata(put);
				destinationTable.put(put);
			}
		} finally {
			rs.close();
		}		
		currentEvaluation = null;
*/		
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
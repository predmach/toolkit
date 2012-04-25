package bigs.core.worker;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;


import bigs.api.core.BIGSException;
import bigs.api.core.State;
import bigs.api.core.Task;
import bigs.api.core.TaskContainer;
import bigs.api.core.TextSerializable;
import bigs.api.data.DataItem;
import bigs.api.storage.DataSource;
import bigs.api.storage.Put;
import bigs.api.storage.Result;
import bigs.api.storage.ResultScanner;
import bigs.api.storage.Scan;
import bigs.api.storage.Table;
import bigs.core.BIGS;
import bigs.core.BIGSProperties;
import bigs.core.pipelines.Pipeline;
import bigs.core.pipelines.PipelineStage;
import bigs.core.pipelines.Schedule;
import bigs.core.pipelines.ScheduleItem;
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
					Table evalTable = BIGS.globalProperties.getPreparedDataSource().getTable(ScheduleItem.tableName);
					Put put = evalTable.createPutObject(scheduleItem.getRowKey());
					scheduleItem.setStatusInProgress();
					put = scheduleItem.fillPutObject(put);
					put = Data.fillInHostMetadata(put);
					Boolean success = evalTable.checkAndPut(scheduleItem.getRowKey(), "bigs", "status", ScheduleItem.statusStrings[ScheduleItem.STATUS_PENDING].getBytes(), put);
					// if we did not succeed in setting the status to pending it is because somebody else did and it is working on it
					if (success) {
						Pipeline pipeline = scheduleItem.getSchedule().getPipelineStage().getPipeline();
						
						if (scheduleItem.getId().equals(0)) {
							pipeline.setTimeStartFromTimeReference();
							pipeline.setTimeDone(null);
							pipeline.save();
						}
						currentScheduleItem = scheduleItem;
						this.doScheduleItem(scheduleItem);
						if (!abort) {
							scheduleItem.setStatusDone();
							scheduleItem.save();
						} else {
							Log.info("schedule item aborted");
							abort = false;
						}
						currentScheduleItem = null;
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


	
	@SuppressWarnings("unchecked")
	public void doScheduleItem(ScheduleItem scheduleItem) {
		
		String methodName = scheduleItem.getMethodName();
		TaskContainer preparedContainer = scheduleItem.getPreparedTaskContainer();
		Task preparedTask = scheduleItem.getPreparedTask();
		Schedule schedule = scheduleItem.getSchedule();
		
		Log.info("doing "+scheduleItem.toString());
		
		if (!scheduleItem.getMethodName().equals("postLoop")) {
			List<Integer> parentsIds = scheduleItem.getParentsIds();
			if (parentsIds.size()>1) {
				throw new BIGSException("schedule item "+scheduleItem.toString()+" can only have one parent and it has "+parentsIds.size());
			}
	
			State previousState = null;
			
			Integer parentId = null;
			if (parentsIds.size()>0) parentId = parentsIds.get(0);
			if (parentId!=null) previousState = schedule.get(parentId).getProcessState();
			
			State resultState = null;
			
			if (methodName.equals(ScheduleItem.METHOD_PRESUBCONTAINERS)) {				
				resultState = preparedContainer.processPreSubContainers(preparedTask, previousState);
			} else if (methodName.equals(ScheduleItem.METHOD_POSTSUBCONTAINERS)) {
				resultState = preparedContainer.processPostSubContainers(preparedTask, previousState);				
			} else if (methodName.equals(ScheduleItem.METHOD_LOOPDATA)) {
				
				preparedContainer.processPreDataBlock(preparedTask, previousState);				
				Map<String, String> tags = scheduleItem.getTags();
				
				PipelineStage stage = scheduleItem.getSchedule().getPipelineStage();
				DataSource inputDataSource    = stage.getPreparedInputDataSource();
				String     inputDataTableName = stage.getInputTableName();
				DataSource outputDataSource    = stage.getPreparedOutputDataSource();
				String     outputDataTableName = stage.getOutputTableName();
						
				DataItem.createDataTableIfDoesNotExist(outputDataSource, outputDataTableName);

				Table inputTable = inputDataSource.getTable(inputDataTableName);
						
				Scan scan = inputTable.createScanObject();
				for (String family: DataItem.dataTableColumnFamilies) {
					scan.addFamily(family);
				}
				
				if (tags!=null) {
					Log.debug("worker on data items with tag");
					for (String tagName: tags.keySet()) {
						String tagValue = tags.get(tagName);
						scan.addFilterByColumnValue("tags", tagName, tags.get(tagName).getBytes());
						Log.debug("    "+tagName+" = "+tagValue);						
					}					
				}
				
				ResultScanner rs = inputTable.getScan(scan);
				
				try {
					for (Result rr = rs.next(); rr!=null; rr = rs.next()) {						
						Log.info("      input  rowkey "+rr.getRowKey());

						DataItem dataItem = DataItem.fromResult(rr);

						Date startTime = new Date();
						//---------------------------------------
						// This is the actual task running
						DataItem outputDataItem = preparedContainer.processDataItem(preparedTask, dataItem);
						//---------------------------------------
						if (abort) {
							Log.error("aborting this evaluation: "+rr.getRowKey());
							return;
						}
						Date endTime = new Date();
						
						scheduleItem.addToElapsedTime(endTime.getTime() - startTime.getTime());

						// store resulting data item if not null
						if (outputDataItem!=null) {
							String destinationRowKey = scheduleItem.getRowKey()+":"+rr.getRowKey();
							Log.info("      output rowkey "+destinationRowKey);
							
							outputDataItem.setRowkey(destinationRowKey);
							outputDataItem.save(outputDataSource, outputDataTableName);
						}
					}
					
					resultState = preparedContainer.processPostDataBlock(preparedTask);
				} finally {
					rs.close();
				}
			} else if (methodName.equals(ScheduleItem.METHOD_PRELOOP)) {
				resultState = preparedContainer.processPreLoop(preparedTask, previousState);
			}
			
			scheduleItem.setProcessState(resultState);
		} else if (methodName.equals(ScheduleItem.METHOD_POSTLOOP)){
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
			
			State resultState = preparedContainer.processPostLoop(preparedTask, parentsStates);
			scheduleItem.setProcessState(resultState);
		}
		
		Long minTime = 0L;
		Long maxTime = 1L;
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
                        DataSource bigsDataSource = BIGS.globalProperties.getPreparedDataSource();
                        if (abort) {
                        	Log.debug("abort programeed. skipping worker alive update");
                        	continue;
                        }
                        if (bigsDataSource!=null && worker.currentScheduleItem!=null) {                        	
                                synchronized(worker.currentScheduleItem) {
                                	ScheduleItem storedScheduleItem = ScheduleItem.load(BIGS.globalProperties.getPreparedDataSource(), currentScheduleItem.getSchedule(), currentScheduleItem.getRowKey());
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
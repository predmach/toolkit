package bigs.core.pipelines;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import org.apache.hadoop.hbase.util.Bytes;

import bigs.api.core.BIGSException;
import bigs.api.core.BIGSParam;
import bigs.api.core.State;
import bigs.api.core.Task;
import bigs.api.core.TaskContainer;
import bigs.api.data.DataItem;
import bigs.api.storage.DataSource;
import bigs.api.storage.Get;
import bigs.api.storage.Put;
import bigs.api.storage.Result;
import bigs.api.storage.Table;
import bigs.api.utils.TextUtils;
import bigs.core.BIGS;
import bigs.core.utils.Core;
import bigs.core.utils.Data;
import bigs.core.utils.Log;
import bigs.core.utils.Text;



public class ScheduleItem {
	
	public final static String tableName = "schedules";
	public final static String[] columnFamilies = new String[]{ "scheduling", "bigs", "content", "tags" };	
	
	
	public static Integer STATUS_PENDING     = 0;
	public static Integer STATUS_INPROGRESS  = 1;
	public static Integer STATUS_DONE        = 2;
	public static Integer STATUS_FAILED      = 3;
	
	public static String METHOD_PRESUBCONTAINERS = "preSubContainers";
	public static String METHOD_POSTSUBCONTAINERS = "postSubContainers";
	public static String METHOD_PRELOOP          = "preLoop";
	public static String METHOD_POSTLOOP         = "postLoop";
	public static String METHOD_LOOPDATA         = "LOOP data";
	
	public static String[] statusStrings = new String[]{ " PENDING  ",
		                                                 "INPROGRESS",
		                                                 "   DONE   ",
		                                                 "  FAILED  " };
	
	String methodName;
	List<String> parentsRowkeys = new ArrayList<String>();
	Schedule schedule;
	String rowkey;

	Date lastUpdate  = new Date(Core.getTime());
	Long elapsedTime = 0L;
	
	String uuiStored = "";
	String hostnameStored = "";

	Task preparedTask;
	TaskContainer<? extends Task> preparedTaskContainer;
	
	Map<String, String> tags = new HashMap<String, String>();
	
	Integer status = ScheduleItem.STATUS_PENDING;
	
	State processState;
	
	public ScheduleItem() {}
	
	public ScheduleItem(Schedule schedule) {
		this.schedule = schedule;
		schedule.addItem(this);	
	}
				
	public ScheduleItem (Schedule schedule, 
						 TaskContainer<? extends Task> configuredTaskContainer, 						
						 Task configuredTask, 
						 String methodName) {
		this(schedule);
		this.preparedTask = configuredTask;
		this.methodName = methodName;
		this.preparedTaskContainer = configuredTaskContainer;
	}
	
	public String getRowkey() {
		return this.rowkey;
	}
	
	public void setRowkey(String rowkey) {
		this.rowkey = rowkey;
	}
	
	public void buildRowkey(Integer thisItemId) {
		String explorationNumber = Text.zeroPad(new Long(this.schedule.pipelineStage.pipeline.getPipelineNumber()), 5);
		String stageNumber = Text.zeroPad(new Long(this.schedule.pipelineStage.getStageNumber()),5);
		String scheduleItemNumber = Text.zeroPad(new Long(thisItemId), 5);	
		this.rowkey = explorationNumber + "." + stageNumber + "." + scheduleItemNumber;		
	}
	
	public Schedule getSchedule() {
		return schedule;
	}
	
	public State getProcessState() {
		return this.processState;
	}
	
	public void setProcessState(State processState) {
		this.processState = processState;
	}
	
	public Task getPreparedTask() {
		return this.preparedTask;
	}
	
	public TaskContainer<? extends Task> getPreparedTaskContainer() {
		return this.preparedTaskContainer;
	}
	
	public String getMethodName() {
		return this.methodName;
	}	
	
	public void setMethodName(String methodName) {
		this.methodName = methodName;
	}
	
	public Boolean isStatusPending() {
		return status == ScheduleItem.STATUS_PENDING;
	}
	
	public Boolean isStatusInProgress() {
		return status == ScheduleItem.STATUS_INPROGRESS;
	}
	
	public Boolean isStatusDone() {
		return status == ScheduleItem.STATUS_DONE;
	}
	
	public Boolean isStatusFailed() {
		return status == ScheduleItem.STATUS_FAILED;				
	}
	
	public void setStatusPending() {
		status = ScheduleItem.STATUS_PENDING;
	}

	public void setStatusInProgress() {
		status = ScheduleItem.STATUS_INPROGRESS;
	}
	
	public void setStatusDone() {
		status = ScheduleItem.STATUS_DONE;
	}

	public void setStatusFailed() {
		status = ScheduleItem.STATUS_FAILED;
	}

	public Integer getStatus() {
		return status;
	}
	
	public Date getLastUpdate() {
		return lastUpdate;
	}
	
	public void setLastUpdate(Date lastUpdate) {
		this.lastUpdate = lastUpdate;
	}
	
	public void setLastUpdateFromString(String lastUpdateString) {
		if (lastUpdateString==null || lastUpdateString.isEmpty()) {
			lastUpdate = null;
			return;
		}		try {
			lastUpdate = TextUtils.FULLDATE.parse(lastUpdateString);
		} catch (ParseException e) {
			throw new BIGSException("error parsing date "+lastUpdateString);
		}
	}

	public void setUuidStored(String uuidStored) {
		this.uuiStored = uuidStored;
	}
	
	/**
	 * this is the UUID of a schedule item retrieved from the DB that has not necessarily been created/updated
	 * by this process.
	 * @return
	 */
	public String getUuidStored() {
		return this.uuiStored;
	}
	
	public void setHostnameStored (String hostnameStored) {
		this.hostnameStored = hostnameStored;
	}

	/**
	 * this is the hostname of a schedule item retrieved from the DB that has not necessarily been created/updated
	 * by this process.
	 * @return
	 */
	public String getHostnameStored( ) {
		return this.hostnameStored;
	}
	public Long getElapsedTime() {
		return elapsedTime;
	}
	
	public void setElapsedTime(Long elapsedTime) {
		this.elapsedTime = elapsedTime;
	}
	
	public void addToElapsedTime(Long time) {
		this.elapsedTime = this.elapsedTime + time;
	}
	
	public String getStatusAsString() {
		return ScheduleItem.statusStrings[this.status];
	}
	
	public void setStatusFromString(String statusString) {
		for (int i=0; i<ScheduleItem.statusStrings.length; i++) {
			if (statusString.trim().equals(ScheduleItem.statusStrings[i].trim())) {
				this.status = i;
				return;
			}
		}
		throw new BIGSException("status "+statusString.trim()+" not recognized for a schedule item");
	}

	public ScheduleItem addParentRowkey(String parentRowkey) {
		if (parentRowkey!=null) {
			this.parentsRowkeys.add(parentRowkey);
		}
		return this;
	}
	
	public ScheduleItem addParentsRowkey (List<String> parentsRowkeys) {
		for (String i: parentsRowkeys) this.parentsRowkeys.add(i);
		return this;
	}
	
	public List<String> getParentsRowkeys() {
		return this.parentsRowkeys;
	}
	
	List<ScheduleItem> getParents() {
		List<ScheduleItem> r = new ArrayList<ScheduleItem>();
		for (String parentRowkey: this.getParentsRowkeys()) {
System.out.println(this.getRowkey()+" checking for parent "+parentRowkey);			
			ScheduleItem item = this.getSchedule().get(parentRowkey);
			// if the item was not found, try to load it directly from DB as it
			// probably belongs to another stage
			if (item==null) {
				DataSource dataSource = BIGS.globalProperties.getPreparedDataSource();
				item = ScheduleItem.load(dataSource, null, parentRowkey);
				Log.info("parent "+parentRowkey+" to item "+this.rowkey+" loaded from datasource");
			}
			if (item!=null) r.add(item);
		}
		return r;
	}
	
	public void addTag(String key, String value) {
		tags.put(key, value);
	}
	
	public void addTags(Map<String, String> map) {
		for (String k: map.keySet()) {
			tags.put(k, map.get(k));
		}
	}
	
	public Map<String, String> getTags() {
		return this.tags;
	}
	
	public void setTags(Map<String, String> tags) {
		this.tags = tags;
	}
		

	/**
	 * builds an empty SchedueItem object from the string representation of its rowkey.
	 * Leaves the task and task container (null)
	 * @param k
	 * @return
	 */
	static public ScheduleItem fromRowKey(Schedule schedule, String k) {	
		
		String[] s = k.split("\\.");
		if (s.length!=3) {
			throw new BIGSException("incorrect schedule item rowkey specification");
		}
		Integer pipelineNumber = new Integer(s[0]);
		Integer stageNumber = new Integer(s[1]);
		
		ScheduleItem r = null;
		if (schedule!=null) {
			if (!schedule.pipelineStage.getStageNumber().equals(stageNumber) ||
				!schedule.pipelineStage.pipeline.getPipelineNumber().equals(pipelineNumber)) {
				throw new BIGSException("error reconstructing schedule item rowkey "+k+" does not correspond to pipeline "+schedule.pipelineStage.getPipeline().getPipelineNumber()+" / stage "+schedule.pipelineStage.getStageNumber());			
			}
			r = new ScheduleItem(schedule);
		} else {
			r = new ScheduleItem();
		}
		
		r.setRowkey(k);
		return r;
	}
	
	/**
	 * builds an ScheduleItem object from a Result object obtained from a Scan or
	 * Get in the underlying storage
	 * @param result
	 * @return
	 */
	static public ScheduleItem fromResultObject(Schedule schedule, Result result) {
		ScheduleItem r = ScheduleItem.fromRowKey(schedule, result.getRowKey());
		
System.out.println("loading schedule item "+r.getRowkey());		
		byte[] stat = result.getValue("bigs", "status");
		if (stat!=null) {
			r.setStatusFromString(new String(stat));
		}
		
		byte[] etime = result.getValue("bigs", "elapsedtime");
		if (etime!=null) {
			r.setElapsedTime(new Long(new String(etime)));
		}

		byte[] lastupdate = result.getValue("bigs", "lastupdate");
		if (lastupdate!=null) {
			r.setLastUpdateFromString(new String(lastupdate));
		}
		
		byte[] suuid = result.getValue("bigs", "uuid");
		if (suuid!=null) {
			r.setUuidStored(new String(suuid));
		}

		byte[] smethod = result.getValue("scheduling", "method");
		if (smethod!=null) {
			r.setMethodName(new String(smethod));
		}

		byte[] shostname = result.getValue("bigs", "hostname");		
		if (shostname!=null) {
			r.setHostnameStored(new String(shostname));
		}
					
		String parentsIdsString = new String(result.getValue("scheduling", "parents"));
		
		if (parentsIdsString!=null && !parentsIdsString.trim().isEmpty()) {
			r.parentsRowkeys = Text.parseObjectList(parentsIdsString, " ", String.class);
		}
		
		r.preparedTask = TaskHelper.fromResultObject(result, "scheduling", "task.class", "task.object");
		r.processState = State.fromResultObject(result, "content", "class", "data");
		r.preparedTaskContainer = TaskContainer.fromResultObject(result, "scheduling", "task.container.class", "task.container.object");
		
		r.tags = result.getFamilyMap("tags");
		
		return r;
	}
	
	/**
	 * Fills a Put object leaving it ready to persist this schedule item into the underlying storage
	 * @param put the Put object to fill in
	 * @return the same object filled in
	 */
	public Put fillPutObject (Put put) {		

		if (this.preparedTask!=null) {
			TaskHelper.toPutObject(this.preparedTask, put, "scheduling", "task.class", "task.object");
		}
		
		if (this.preparedTaskContainer!=null) {
			this.preparedTaskContainer.toPutObject(put, "scheduling", "task.container.class", "task.container.object");
		}

		if (this.getProcessState()!=null) {
			this.getProcessState().toPutObject(put, "content", "class", "data");
		}
		
		put.add("bigs","status", Bytes.toBytes(this.getStatusAsString()));
		put.add("scheduling", "parents", Bytes.toBytes(Text.collate(this.parentsRowkeys.toArray(), " ")));
		
		put.add("scheduling", "method", Bytes.toBytes(this.getMethodName()));
				
		lastUpdate = new Date(Core.getTime());
    	
		put.add("bigs", "lastupdate", Bytes.toBytes(TextUtils.FULLDATE.format(this.lastUpdate)));
    		
    	if (this.elapsedTime!=null) {
    		put.add("bigs", "elapsedtime", Bytes.toBytes(this.elapsedTime.toString()));
    	}
    	
    	if (this.tags!=null) {
    		for (String k: this.tags.keySet()) {
    			String v = this.tags.get(k);
    			if (v!=null && !v.isEmpty()) {
    				put.add("tags", k, Bytes.toBytes(v));
    			}
    		}
    	}
		return put;
	}
	
	/**
	 * Fills a Get object leaving it ready to completely retrieve this schedule item object from the underlying storage
	 * @param get the Get object to fill in
	 * @return the same Get object filled in
	 */
	public static Get fillGetObject (Get get) {
		
		for (String family: ScheduleItem.columnFamilies) {
			get.addFamily(family);
		}
		get.addColumn("bigs", "timestart");
		get.addColumn("bigs", "timedone");
		get.addColumn("bigs", "lastupdate");
		get.addColumn("bigs", "hostname");
		get.addColumn("bigs", "uuid");
		get.addColumn("scheduling", "task.class");
		get.addColumn("scheduling", "task.object");
		get.addColumn("scheduling", "task.container.object");
		get.addColumn("scheduling", "task.container.object");
		get.addColumn("scheduling", "method");
		get.addColumn("scheduling", "parents");
		get.addColumn("content", "data");
		get.addColumn("content", "class");
		return get;
	}	

	/**
	 * Loads an schedule item from a datasource
	 * 
	 * @param dataSource the datasource object where to look up the schedule item
	 * @param rowKey the rowkey of the schedule item to retrieve
	 * @return
	 */
	static public ScheduleItem load (DataSource dataSource, Schedule schedule, String rowKey) {
		Table table = dataSource.getTable(ScheduleItem.tableName);
		Get get = ScheduleItem.fillGetObject(table.createGetObject(rowKey));
		Result result = table.get(get);
		ScheduleItem r = ScheduleItem.fromResultObject(schedule, result);
		return r;
	}
	
	/**
	 * saves this schedule item in the underlying datasource
	 * @param dataSource the datasource
	 */
	public void save() {
    	DataSource dataSource = BIGS.globalProperties.getPreparedDataSource();
    	Table table = dataSource.getTable(ScheduleItem.tableName);
    	table.put(Data.fillInHostMetadata(this.fillPutObject(table.createPutObject(this.getRowkey()))));				
	}
		
	/**
	 * time stamps this schedule item in the underlying storage by udpating the content
	 * of the column 'bigs:alive' with the current date only if the existing uuid is the
	 * same of this process.
	 * 
	 * @param dataSource the datasource object where this schedule item is stored
	 * @return true if the update was successful, false otherwise (if the existing uuid
	 *         is different, meaning that somebody else is working on this schedule item)
	 */
	public Boolean markAlive(DataSource dataSource) {
		Table table = dataSource.getTable(ScheduleItem.tableName);
		Put put = table.createPutObject(this.getRowkey());
		this.lastUpdate = new Date(Core.getTime());
		put = this.fillPutObject(put);
		put = Data.fillInHostMetadata(put);		
		Boolean r = table.checkAndPut(this.getRowkey(), "bigs", "uuid", Core.myUUID.getBytes(), put);
		return r;
	}
	
	
	public ScheduleItem clone() {
		ScheduleItem r = new ScheduleItem(this.schedule);
		r.preparedTask = this.preparedTask;
		r.preparedTaskContainer = this.preparedTaskContainer;
		r.rowkey = this.rowkey;
		r.parentsRowkeys = this.parentsRowkeys;
		r.hostnameStored = this.hostnameStored;
		r.uuiStored = this.uuiStored;
		r.lastUpdate = this.lastUpdate;
		r.status = this.status;
		r.elapsedTime = this.elapsedTime;
		r.processState = this.processState;
		r.rowkey = this.rowkey;
		return r;
	}
	
	Boolean bothNullOrEqual(Object o1, Object o2) {
		if (o1==null && o2==null) return true;
		if (o1==null || o2==null) return false;
		if (!o1.equals(o2)) return false;
		return true;
	}
	
	public Boolean equals(ScheduleItem si) {
		if (!bothNullOrEqual(this.schedule.pipelineStage.stageNumber, si.schedule.pipelineStage.stageNumber)) return false;
		if (!bothNullOrEqual(this.schedule.pipelineStage.pipeline.getPipelineNumber(), si.schedule.pipelineStage.pipeline.getPipelineNumber())) return false;

		if (!bothNullOrEqual(this.status, si.status)) return false;
		if (!bothNullOrEqual(this.elapsedTime, si.elapsedTime)) return false;
		
		return true;
	}		
	
	
	/**
	 * Checks if the schedule item is available to start working on it.
	 * Returns true if all the schedule item parents are done.
	 */
	public boolean canProcess() {
		if (this.isStatusPending()) {
			Boolean anyParentNotFinished = false;
			for (ScheduleItem parent: this.getParents()) {
				if (parent==null) {
					throw new BIGSException("parent for schedule item "+this.getRowkey()+" not found");
				}
				if (!parent.isStatusDone()) {
					anyParentNotFinished = true;
					break;
				}
			}
			
			return !anyParentNotFinished;		
		}
		return false;
	}
		
	public String toString() {
		String r = this.getRowkey()+" "+preparedTaskContainer.toString()+ " "+	preparedTask.toString() + " " + methodName;
		return r;
	}
	
}
